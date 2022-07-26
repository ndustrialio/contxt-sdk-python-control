import json
from datetime import datetime
from typing import Any, AnyStr, Dict, List, Optional

import pytz
from sgqlc.operation import Operation

from contxt.services.base_graph_service import BaseGraphService, SchemaMissingException
from contxt.utils.config import ContxtEnvironmentConfig

try:
    import contxt.schemas.foundry_graph.foundry_graph_schema as schema
    from contxt.schemas.foundry_graph.foundry_graph_schema import ComponentToControlInputRecordInput
except ImportError:
    raise SchemaMissingException(
        "[ERROR] Schema is not generated for GraphQL -- run `contxt init` to "
        "initialize then re-run the command"
    )

from contxt.models.control import Suggestion


def include_proposals_with_object(obj, include_only_active: bool = True, project_id: str = None):
    filters = {}
    if project_id:
        filters["project_id"] = project_id
    proposals = obj.event_proposals(
        condition=filters, order_by=[schema.EventProposalsOrderBy.START_TIME_ASC]
    ).nodes()
    proposals.id()
    proposals.start_time()
    proposals.end_time()
    proposals.current_state()


class ControlService(BaseGraphService):
    def __init__(self, contxt_env: ContxtEnvironmentConfig):
        super().__init__(contxt_env)

    def create_definition_from_json_file(
        self, json_file: str, slug: str, project_id: str, label: str, description: str
    ) -> schema.StateDefinition:
        print(f"Opening json file at {json_file}")
        with open(json_file, "r") as f:
            definition_data = json.load(f)

            return self.create_definition(
                json_obj=definition_data,
                slug=slug,
                project_id=project_id,
                label=label,
                description=description,
            )

    def get_event_proposals(self, facility_id: int, project_id: str = None, filters: dict = None):
        op = Operation(schema.Query)

        if filters is None:
            filters = {}

        if facility_id:
            filters["facility_id"] = {"equalTo": facility_id}

        if project_id:
            filters["project_id"] = {"equalTo": project_id}

        if len(filters) > 0:
            proposals = op.event_proposals(
                filter=filters,
                order_by=[schema.EventProposalsOrderBy.START_TIME_ASC],
            ).nodes()
        else:
            proposals = op.event_proposals(
                order_by=[schema.EventProposalsOrderBy.START_TIME_ASC]
            ).nodes()

        proposals.id()
        proposals.facility().name()
        proposals.facility().id()
        proposals.start_time()
        proposals.end_time()
        proposals.current_state()
        proposals.metadata()
        proposals.bitly_link()

        # include projects
        proposals.project.id()
        proposals.project.name()

        # include metrics
        proposals.event_proposal_metrics().nodes().id()
        proposals.event_proposal_metrics().nodes().actual_impact_amount()
        proposals.event_proposal_metrics().nodes().projected_impact_amount()
        proposals.event_proposal_metrics().nodes().calculation_metadata()
        proposals.event_proposal_metrics().nodes().controllable_component().id()
        proposals.event_proposal_metrics().nodes().controllable_component().slug()

        data = self._get_endpoint()(op)
        if "errors" in data:
            print(data)
            raise Exception(data["errors"][0]["message"])

        proposals = (op + data).event_proposals

        return proposals.nodes

    def get_proposal_detail(
        self, event_proposal_id: str, include_event_log: bool = True, include_metrics: bool = True
    ):
        op = Operation(schema.Query)

        event_proposal = op.event_proposal(id=event_proposal_id)

        event_proposal.id()
        event_proposal.facility_id()
        event_proposal.start_time()
        event_proposal.approval_deadline_time()
        event_proposal.control_start_deadline_time()
        event_proposal.end_time()
        event_proposal.current_state()
        event_proposal.metadata()
        event_proposal.bitly_link()

        # Include project info
        event_proposal.project().id()

        # Include component info
        event_proposal.event_proposals_controlled_components().nodes().state_definition()
        event_proposal.event_proposals_controlled_components().nodes().controllable_component().id()
        event_proposal.event_proposals_controlled_components().nodes().controllable_component().slug()
        event_proposal.event_proposals_controlled_components().nodes().controllable_component().label()

        # Include control events
        event_proposal.control_events().nodes().id()
        event_proposal.control_events().nodes().current_state()
        event_proposal.control_events().nodes().start_time()
        event_proposal.control_events().nodes().end_time()

        # Include metrics
        if include_metrics:
            event_proposal.event_proposal_metrics().nodes().id()
            event_proposal.event_proposal_metrics().nodes().actual_impact_amount()
            event_proposal.event_proposal_metrics().nodes().projected_impact_amount()
            event_proposal.event_proposal_metrics().nodes().calculation_metadata()
            event_proposal.event_proposal_metrics().nodes().controllable_component().id()
            event_proposal.event_proposal_metrics().nodes().controllable_component().slug()

        # Include events
        if include_event_log:
            logs = event_proposal.event_proposal_logs(
                order_by=[schema.EventProposalLogsOrderBy.EVENT_TIME_ASC]
            ).nodes()
            logs.event_time()
            logs.event_type()
            logs.label()
            logs.by_user_id()
            logs.previous_state()
            logs.current_state()
            logs.data()

        data = self._get_endpoint()(op)

        event_proposal = (op + data).event_proposal

        return event_proposal

    def get_latest_proposal_for_component(
        self, controllable_component_id: str, project_id: str = None
    ) -> Optional[schema.EventProposal]:
        component_with_proposals = self.get_proposals_for_component(
            controllable_component_id, limit=1, project_id=project_id
        )

        if len(component_with_proposals.event_proposals.nodes):
            return component_with_proposals.event_proposals.nodes[0]

    def get_proposals_for_component(
        self, controllable_component_id: str, limit=None, project_id: str = None
    ) -> schema.ControllableComponent:
        op = Operation(schema.Query)

        controllable_component = op.controllable_component(id=controllable_component_id)
        controllable_component.id()
        controllable_component.slug()

        filters = {}
        if project_id:
            filters["project_id"] = project_id

        proposals = controllable_component.event_proposals(
            condition=filters, order_by=[schema.EventProposalsOrderBy.START_TIME_DESC], first=limit
        ).nodes()
        proposals.id()
        proposals.project_id()
        proposals.current_state()
        proposals.metadata()
        proposals.start_time()
        proposals.end_time()
        proposals.summary()

        data = self.run(op)

        return (op + data).controllable_component

    def get_control_event_detail(self, control_event_id: str):
        op = Operation(schema.Query)

        control_event = op.control_event(id=control_event_id)

        control_event.start_time()
        control_event.end_time()
        control_event.current_state()

        # Audit Logs
        logs = control_event.control_event_logs(
            order_by=[schema.ControlEventLogsOrderBy.EVENT_TIME_ASC]
        ).nodes()
        logs.event_time()
        logs.event_type()
        logs.label()
        logs.by_user_id()
        logs.by_edge_node_id()
        logs.previous_state()
        logs.current_state()
        logs.data()

        data = self._get_endpoint()(op)

        control_event = (op + data).control_event

        return control_event

    def get_edge_control_events(self):
        op = Operation(schema.Query)

        edge_control_events = op.edge_control_events()

        edge_control_events.nodes.componentslug()
        control_event = edge_control_events.nodes.controlevent()

        control_event.id()
        control_event.start_time()
        control_event.end_time()
        control_event.state_machine().state_definition()
        control_event.state_machine().current_state()

        data = self.run(op)

        edge_control_events = (op + data).edge_control_events

        return edge_control_events

    def send_proposal_reviewer_notification(self, proposal_id: str, message: str):
        op = Operation(schema.Mutation)

        notification_input = schema.ProposalReviewerNotificationInput()
        notification_input.proposal_id = proposal_id
        notification_input.message = message

        notification = op.send_proposal_reviewer_notification(input=notification_input)

        notification.event_proposal.id()

        data = self._get_endpoint()(op)
        if 'errors' in data:
            raise Exception(data['errors'][0]['message'])

        event = (op + data).send_proposal_reviewer_notification
        return event

    def transition_event(self, control_event_id: str, transition_event: str, message: str = None):
        op = Operation(schema.Mutation)

        transition_input = schema.TransitionControlEventInput()
        transition_input.control_event_id = control_event_id
        transition_input.transition_event = transition_event
        transition_input.message = message

        transition = op.transition_control_event(input=transition_input)

        transition.control_event.id()
        transition.control_event.state_machine().current_state()

        data = self._get_endpoint()(op)
        if "errors" in data:
            raise Exception(data["errors"][0]["message"])

        event = (op + data).transition_control_event
        return event

    def get_definitions(self, definition_slug: str):
        op = Operation(schema.Query)

        if definition_slug:
            query = op.state_definition(slug=definition_slug)
        else:
            definitions = op.state_definitions()
            query = definitions.nodes()

        query.slug()
        query.definition()

        data = self._get_endpoint()(op)

        if definition_slug:
            definitions = (op + data).state_definition
        else:
            definitions = (op + data).state_definitions

        return definitions

    def create_definition(
        self, json_obj, slug: str, project_id: str, description: str, label: str
    ) -> schema.StateDefinition:
        op = Operation(schema.Mutation)

        def_input = schema.StateDefinitionInput()
        def_input.definition = json.dumps(json_obj)
        def_input.project_id = project_id
        def_input.slug = slug
        def_input.description = description
        def_input.label = label

        create_def_input = schema.CreateStateDefinitionInput(state_definition=def_input)

        create = op.create_state_definition(input=create_def_input)

        create.state_definition.slug()
        create.state_definition.project_id()
        create.state_definition.label()
        create.state_definition.description()

        data = self.run(op)

        return (op + data).create_state_definition.state_definition

    def add_historic_event(
        self,
        facility_id: int,
        project_id: str,
        start_time: datetime,
        end_time: datetime,
        components: List[schema.ControllableComponent],
    ):

        op = Operation(schema.Mutation)

        historic_event = schema.AddHistoricEventInput()
        historic_input = schema.HistoricEventProposalInputRecordInput()
        historic_input.facilityid = facility_id
        historic_input.projectid = project_id
        historic_input.starttime = str(start_time.astimezone(pytz.utc))
        historic_input.endtime = str(end_time.astimezone(pytz.utc))
        historic_event.historic_event = historic_input
        historic_event.components = components

        propose = op.add_historic_event(input=historic_event)

        propose.event_proposal.id()

        data = self._get_endpoint()(op)
        if "errors" in data:
            print(data)
            raise Exception(data["errors"][0]["message"])

        event = (op + data).add_historic_event.event_proposal
        return event

    def submit_suggestion(self, suggestion: Suggestion) -> schema.EventProposal:

        inputs = []
        for component in suggestion.components:
            inputs.append(
                ComponentToControlInputRecordInput(
                    controllable_component_id=component.id,
                    state_definition_slug=component.state_definition_slug,
                )
            )

        return self.propose_event(
            summary=suggestion.summary,
            facility_id=suggestion.facility_id,
            start_time=suggestion.start_time,
            end_time=suggestion.end_time,
            components=inputs,
            project_id=suggestion.project_id,
            metadata=suggestion.metadata,
            control_start_deadline_time=suggestion.control_start_deadline_time,
            approval_deadline_time=suggestion.approval_deadline_time,
            start_control_upon_approval=suggestion.start_control_upon_approval,
        )

    def adjust_proposal_end(self, proposal_id: str, new_end_time: datetime) -> schema.EventProposal:
        op = Operation(schema.Mutation)

        adjustment_input = schema.AdjustProposalEndTimeInputRecordInput()
        adjustment_input.proposalid = proposal_id
        adjustment_input.newendtime = str(new_end_time)

        adjust = schema.AdjustProposalEndTimeInput()
        adjust.proposal_adjustment = adjustment_input

        operation = op.adjust_proposal_end_time(input=adjust)

        operation.event_proposal.id()
        operation.event_proposal.current_state()
        operation.event_proposal.end_time()

        data = self.run(op)

        proposal = (op + data).adjust_proposal_end_time.event_proposal
        return proposal

    def update_metadata(self, proposal_id: str, metadata: Dict[AnyStr, Any]) -> schema.EventProposal:
        op = Operation(schema.Mutation)

        update_input = schema.UpdateProposalMetadataInput()
        update_input.proposalid = proposal_id
        update_input.proposalmetadata = json.dumps(metadata)

        operation = op.update_proposal_metadata(input=update_input)

        operation.event_proposal.id()
        operation.event_proposal.current_state()
        operation.event_proposal.end_time()

        data = self.run(op)

        proposal = (op + data).update_proposal_metadata.event_proposal
        return proposal

    def propose_event(
        self,
        facility_id: int,
        project_id: str,
        start_time: datetime,
        end_time: datetime,
        components: List[ComponentToControlInputRecordInput],
        summary: str,
        control_start_deadline_time: Optional[datetime] = None,
        approval_deadline_time: Optional[datetime] = None,
        metadata: Dict[AnyStr, Any] = None,
        start_control_upon_approval: Optional[bool] = None,
    ) -> schema.EventProposal:

        op = Operation(schema.Mutation)

        event_proposal = schema.EventProposalInputRecordInput()
        event_proposal.facilityid = facility_id
        event_proposal.projectid = project_id
        event_proposal.starttime = str(start_time.astimezone(pytz.utc))
        event_proposal.endtime = str(end_time.astimezone(pytz.utc))
        if start_control_upon_approval:
            event_proposal.startcontroluponapproval = start_control_upon_approval
        if control_start_deadline_time:
            event_proposal.controlstartdeadlinetime = str(control_start_deadline_time)
        if approval_deadline_time:
            event_proposal.approvaldeadlinetime = str(approval_deadline_time)
        event_proposal.summary = summary
        event_proposal.metadata = json.dumps(metadata)

        proposal = schema.ProposeEventInput(
            proposed_event=event_proposal, components_to_control=components
        )

        propose = op.propose_event(input=proposal)

        propose.event_proposal.id()
        data = self.run(op)

        event = (op + data).propose_event.event_proposal
        return event

    def approve_proposal(
        self, proposal_id: str, components: List[ComponentToControlInputRecordInput]
    ) -> schema.EventProposal:

        op = Operation(schema.Mutation)

        approved_proposal = schema.EventApprovalInputRecordInput()
        approved_proposal.id = proposal_id

        approval = schema.ApproveProposalInput(
            approved_proposal=approved_proposal, approved_components=components
        )

        approve = op.approve_proposal(input=approval)

        approve.event_proposal.id()
        approve.event_proposal.start_time()
        approve.event_proposal.end_time()
        data = self.run(op)

        event = (op + data).approve_proposal.event_proposal
        return event

    def add_savings_for_component_control_event(
        self,
        component: schema.ControllableComponent,
        event_proposal: schema.EventProposal,
        success_metric_id: str,
        projected_savings_amount: float,
    ):
        op = Operation(schema.Mutation)

        input = schema.CreateEventProposalMetricInput()
        metric = schema.EventProposalMetricInput()
        metric.event_proposal_id = event_proposal.id
        metric.controllable_component_id = component.id
        metric.project_success_metric_id = success_metric_id
        metric.projected_impact_amount = projected_savings_amount

        input.event_proposal_metric = metric

        new_metric = op.create_event_proposal_metric(input=input)

        new_metric.event_proposal_metric.project_success_metric_id()

        data = self._get_endpoint()(op)

        if "errors" in data:
            print(data)
            raise Exception(data["errors"][0]["message"])

        metric = (op + data).create_event_proposal_metric
        return metric

    def add_actual_savings_for_component_control_event(
        self, success_metric_id: str, actual_savings_amount: float, metadata: str
    ):
        op = Operation(schema.Mutation)

        update = schema.UpdateEventProposalMetricInput()
        patch = schema.EventProposalMetricPatch()

        update.id = success_metric_id
        patch.actual_impact_amount = actual_savings_amount
        if metadata:
            patch.calculation_metadata = metadata

        update.patch = patch

        update_metric = op.update_event_proposal_metric(input=update)
        update_metric.event_proposal_metric.id()

        data = self._get_endpoint()(op)

        if "errors" in data:
            print(data)
            raise Exception(data["errors"][0]["message"])

        metric = (op + data).update_event_proposal_metric
        return metric

    def create_facility(self, name: str, id: int, organization_id: str):
        op = Operation(schema.Mutation)

        create = schema.CreateFacilityInput()
        facility = schema.FacilityInput()
        facility.id = id
        facility.name = name
        facility.organization_id = organization_id

        create.facility = facility

        post_create = op.create_facility(input=create)

        post_create.facility.id()
        post_create.facility.name()
        post_create.facility.organization_id()

        data = self._get_endpoint()(op)
        if "errors" in data:
            print(data)
            raise Exception(data["errors"][0]["message"])

        facility = (op + data).create_facility
        return facility

    def set_controllable_as_schedulable(self, controllable_id: str) -> schema.ControllableComponent:
        op = Operation(schema.Mutation)

        update = schema.UpdateControllableComponentInput()
        update.id = controllable_id
        patch = schema.ControllableComponentPatch()
        patch.is_schedulable = True
        update.patch = patch

        update_controllable = op.update_controllable_component(input=update)

        update_controllable.controllable_component.is_schedulable()

        data = self._get_endpoint()(op)
        if "errors" in data:
            print(data)
            raise Exception(data["errors"][0]["message"])

        component = (op + data).update_controllable_component
        return component

    def get_controllable_by_slug(
        self, facility_id: int, component_slug: str
    ) -> Optional[schema.ControllableComponent]:
        components = self.get_controllables_for_facility(
            facility_id=facility_id, component_slug=component_slug, include_events=False
        )

        if len(components):
            return components[0]

    def get_controllables_for_facility(
        self,
        facility_id: int,
        component_slug: str = None,
        include_events: bool = True,
        project_id: str = None,
    ):
        op = Operation(schema.Query)

        filters = {}
        if facility_id:
            filters["facility_id"] = facility_id
        if component_slug:
            filters["slug"] = component_slug

        if len(filters) > 0:
            components = op.controllable_components(condition=filters)
        else:
            components = op.controllable_components()

        if include_events:
            include_proposals_with_object(components.nodes, project_id=project_id)

        components.nodes.id()
        components.nodes.slug()
        components.nodes.label()
        components.nodes.is_schedulable()

        data = self._get_endpoint()(op)

        components = (op + data).controllable_components

        return components.nodes

    def get_project_definition(
        self,
        project_id: str,
        metric_name: str = None,
        include_events: bool = True,
        include_only_enrolled_facilities: bool = True,
    ):
        op = Operation(schema.Query)

        project = op.project(id=project_id)

        project.id()
        project.description()
        project.name()

        if metric_name:
            metrics = project.project_success_metrics(condition={"name": metric_name}).nodes()
        else:
            metrics = project.project_success_metrics().nodes()

        metrics.id()
        metrics.name()
        metrics.units()
        metrics.description()

        if include_events:
            include_proposals_with_object(project)

        if include_only_enrolled_facilities:
            facility_projects = project.facility_projects(
                condition={"enrollmentStatus": "ENROLLED"}
            ).nodes()
        else:
            facility_projects = project.facility_projects().nodes()
        facility_projects.enrollment_status()
        facility = facility_projects.facility()
        facility.name()

        data = self._get_endpoint()(op)

        project = (op + data).project

        return project

    def get_projects(self, facility_id: int):
        op = Operation(schema.Query)

        projects = op.projects()

        projects.nodes().id()
        projects.nodes().name()
        projects.nodes().description()

        data = self._get_endpoint()(op)

        projects = (op + data).projects

        return projects.nodes

    def get_facilities(
        self,
        include_events: bool = True,
    ):
        op = Operation(schema.Query)

        facilities = op.facilities().nodes()

        facilities.id()
        facilities.name()

        # Project Info
        projects = facilities.facility_projects().nodes()
        projects.enrollment_status()

        project = projects.project()
        project.name()

        # Event Info
        if include_events:
            include_proposals_with_object(facilities)

        # User Roles
        controllers = facilities.facility_users().nodes()
        controllers.user_id()
        controllers.role()

        data = self._get_endpoint()(op)

        facilities = (op + data).facilities

        return facilities.nodes

    def get_edge_nodes(self, facility_id: int = None):
        op = Operation(schema.Query)

        filters = {}
        if facility_id:
            filters["facility_id"] = facility_id

        if len(filters) > 0:
            edge_nodes = op.edge_nodes(condition=filters).nodes()
        else:
            edge_nodes = op.edge_nodes().nodes()

        edge_nodes.client_id()
        edge_nodes.facility_id()
        edge_nodes.organization_id()
        edge_nodes.last_fetch_time()

        data = self.run(op)

        edge_nodes = (op + data).edge_nodes

        return edge_nodes.nodes

    def get_edge_node(self, client_id: str):
        op = Operation(schema.Query)

        edge_node = op.edge_node(client_id=client_id)

        edge_node.client_id()
        edge_node.facility_id()
        edge_node.organization_id()
        edge_node.last_fetch_time()

        # Facility Info
        facility = edge_node.facility()
        facility.id()
        facility.name()

        # Organization Info
        org = edge_node.organization()
        org.id()
        org.name()

        # Components
        components = edge_node.controllable_components_by_controlled_by_edge_node_client_id().nodes()
        components.slug()
        components.label()

        data = self.run(op)

        edge_node = (op + data).edge_node
        return edge_node

    def get_facility(self, id: int, include_events: bool = True):
        op = Operation(schema.Query)

        facility = op.facility(id=id)

        facility.id()
        facility.name()
        facility.organization_id()

        # Controllable Info
        controllables = facility.controllable_components().nodes()
        controllables.slug()
        controllables.label()
        controllables.description()

        # Control Event Info
        if include_events:
            include_proposals_with_object(facility)

        data = self._get_endpoint()(op)

        facility = (op + data).facility

        return facility

    def get_organizations(self):
        op = Operation(schema.Query)

        orgs = op.organizations().nodes()

        orgs.id()
        orgs.name()

        data = self._get_endpoint()(op)

        orgs = (op + data).organizations

        return orgs.nodes

    def enroll_facility(self, facility_id: int, project_id: str):
        query = """
            mutation addFacilityProject($projectId: String!, $facilityId: Int!) {
              createFacilityProject(input: {
                facilityProject: {
                  projectId: $projectId,
                  facilityId: $facilityId,
                  enrollmentStatus: "ENROLLED"
                }
              }) {
                facilityProject {
                  id
                }
              }
            }
        """

        return self.run_query(query, {'projectId': project_id,
                                      'facilityId': facility_id})

    def create_component(self, label: str, slug: str, description: str, facility_id: int, controlled_by_edge_client_id: str = None) -> str:
        query = """
            mutation createComponent($label: String!, $slug: String!, $description: String!, $facilityId: Int!, $controllingEdgeNode: String) {
              createControllableComponent(input: {
                controllableComponent: {
                  controlledByEdgeNodeClientId: $controllingEdgeNode,
                  label: $label,
                  slug: $slug,
                  description: $description,
                  facilityId: $facilityId
                }
              }) {
                controllableComponent {
                  id
                }
              }
            }
        """

        data = self.run_query(query, {'label': label,
                                      'slug': slug,
                                      'description': description,
                                      'facilityId': facility_id,
                                      'controllingEdgeNode': controlled_by_edge_client_id})

        return data['createControllableComponent']['controllableComponent']['id']

