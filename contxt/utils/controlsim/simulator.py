from typing import List, Dict, Optional
import traceback
import pytz
from dateutil import parser
import time
from datetime import datetime, timedelta

from contxt.utils.config import load_config_class_from_file, ContxtEnvironmentConfig
from contxt.utils.contxt_environment import ContxtEnvironment
from contxt.services.control.control import ControlService
from dataclasses import dataclass

from contxt.utils.controlsim.models import SimulationConfigs, DefinitionConfig, SimulatedStateRunMode

LEAD_BUFFER_TIME_MINUTES = 3


class SimulatorException(Exception):
    pass


def get_control_service(control_config: ContxtEnvironmentConfig = None):
    if not control_config:
        contxt_env = ContxtEnvironment()
        control_config = contxt_env.get_config_for_service_name('foundry-graph')
    if control_config.clientId is None:
        raise SimulatorException("Can't use CLI Auth for authentication in simulator. Must use the credentials of "
                                 "the Edge Node. Use `contxt env set-context foundry-graph` to select a set of Edge "
                                 "Node credentials to use")
    return ControlService(contxt_env=control_config)


@dataclass
class FrameworkState:
    is_stale: bool
    control_event_id: str
    end_time: datetime
    state: str


class GeneralControlEventSimulator:

    def __init__(self,
                 control_event,
                 config: DefinitionConfig,
                 control: ControlService,
                 component_slug: str,
                 event_hooks: dict[str, classmethod]
                 ):
        self.control_event = control_event
        self.end_time : datetime = parser.parse(self.control_event.end_time)
        self.config = config
        self.control = control
        self.my_component = component_slug
        self.event_hooks = event_hooks
        print(f'Registered event hooks: {self.event_hooks}')
        self.next_transition_time: Optional[datetime] = None
        self.current_state: str = None

    def check(self, framework_state: FrameworkState):
        # grab our current (initial state)
        self.current_state = framework_state.state

        # check on the state of things every second. if a simulated timer is running, then move on
        state_config = self.config.get_state_config(self.current_state)

        if not state_config:
            print(f'Config not found for state {self.current_state}')
            return

        # check to see if our end time changed
        if self.end_time != framework_state.end_time:
            print(f'End time from the API changed...making change on our side')
            self.end_time = framework_state.end_time


        # if we're in a state of waiting for external input, we'll grab the state from the framework
        if not state_config.controllable:
            # can't do this since the framework state may be out of date
            if framework_state.is_stale:
                print('States out of sync. Waiting for sync')
                return
            else:
                self.current_state = framework_state.state

            print(f'[{self.my_component}] -- current state is {self.current_state}')
            print('Waiting on external input -- not controllable by our system')

        elif self.next_transition_time is not None and datetime.now(tz=pytz.UTC) > self.next_transition_time:
            self.transition(framework_state)
        else:
            # if a transition time has not been set for the next transition, let's set it
            if not self.next_transition_time:
                # if we're using a generic time delay
                if state_config.delay:
                    seconds_delay = state_config.delay
                    self.next_transition_time = datetime.now(tz=pytz.UTC) + timedelta(seconds=seconds_delay)
                # if we're running until the end
                elif state_config.mode == SimulatedStateRunMode.runUntilEndTime:
                    self.next_transition_time = self.end_time
                else:
                    print('Invalid simulator configuration')
                print(f'[{self.my_component}] -- {state_config.workMessage}')
                print(f'[{self.my_component}] -- Setting timer to send {state_config.onSuccess} at '
                      f'{self.next_transition_time}')

        # call any hooks we may have registered
        if self.current_state in self.event_hooks:
            func = self.event_hooks.get(self.current_state)
            # Call the function
            func(framework_state, self.control_event)

    def transition(self, framework_state: FrameworkState):
        state_config = self.config.get_state_config(self.current_state)
        event = state_config.onSuccess
        control_object = framework_state
        transition = self.control.transition_event(control_event_id=control_object.control_event_id,
                                                   transition_event=event)
        control_object.is_stale = True
        self.next_transition_time = None
        print(transition)
        self.current_state = transition.control_event.state_machine.current_state
        print(f'Event Transition: {event}')
        print(transition)


class Simulator:

    def __init__(self,
                 definitions: List[str],
                 simulator_config_filename: str,
                 event_hooks: dict[str, classmethod] = None
                 ):
        self.simulation_config: SimulationConfigs = load_config_class_from_file(simulator_config_filename, SimulationConfigs)
        self.control_service = get_control_service()
        self.definitions = definitions
        self.event_hooks = event_hooks if not None else {}
        self.framework_reported_current_states : Dict[str, FrameworkState] = {}
        self.events_to_monitor: dict[str, GeneralControlEventSimulator] = {}

    # Thread for querying the controls api every 5 seconds and keeping our worker threads up to date
    def run(self):
        print('Running')
        while True:
            # sleep for 5 seconds before fetching more
            time.sleep(5)
            try:
                print('Fetching control events')
                events = self.control_service.get_edge_control_events()

                active_framework_events = []
                # iterate over the events and persist framework state to global config
                for event in events.nodes:
                    active_framework_events.append(event.controlevent.id)

                    # if there is not a thread already for this, let's create one
                    if event.componentslug not in self.events_to_monitor:
                        print(f'{event.componentslug}')
                        print(f'  {event.controlevent.state_machine.state_definition}')
                        print(f'  {event.controlevent.state_machine.current_state}')
                        definition_config = self.simulation_config.get_definition_config_for_slug(event.controlevent.state_machine.state_definition)
                        if not definition_config:
                            print(f'Definition config not found for '
                                  f'{event.controlevent.state_machine.state_definition}')
                            continue

                        print(event.controlevent.start_time)
                        start_time = parser.parse(event.controlevent.start_time)
                        print(start_time, datetime.now(tz=pytz.UTC))
                        if start_time - timedelta(minutes=LEAD_BUFFER_TIME_MINUTES) > datetime.now(tz=pytz.UTC):
                            print(f'Have not reached start time yet...')
                            continue
                        self.framework_reported_current_states[event.componentslug] = \
                            FrameworkState(is_stale=False,
                                           control_event_id=event.controlevent.id,
                                           end_time=parser.parse(event.controlevent.end_time),
                                           state=event.controlevent.state_machine.current_state)
                        self.events_to_monitor[event.componentslug] = \
                            GeneralControlEventSimulator(config=definition_config,
                                                         control=self.control_service,
                                                         component_slug=event.componentslug,
                                                         control_event=event.controlevent,
                                                         event_hooks=self.event_hooks)
                    else:
                        current_framework_state = self.framework_reported_current_states[event.componentslug]
                        if current_framework_state.control_event_id != event.controlevent.id:
                            print('Different control event ID detected. Resetting for next cycle')
                            del self.events_to_monitor[event.componentslug]
                            continue
                        self.framework_reported_current_states[event.componentslug] = \
                            FrameworkState(is_stale=False,
                                           control_event_id=event.controlevent.id,
                                           end_time=parser.parse(event.controlevent.end_time),
                                           state=event.controlevent.state_machine.current_state)

                events_to_delete = []
                # run through all our events and let them check for things that need to be done
                for component, event in self.events_to_monitor.items():
                    # the API is no longer tracking this item so we need to kill it
                    if event.control_event.id not in active_framework_events:
                        events_to_delete.append(component)
                    else:
                        # otherwise all good -- let's check it
                        event.check(self.framework_reported_current_states[component])

                # delete the events we don't need anymore
                for component in events_to_delete:
                    del self.events_to_monitor[component]

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(traceback.format_exc())
                print(f'Handled exception {e}')
        print('Stopping simulator')
