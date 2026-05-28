"""
app/admin/views.py — SQLAdmin ModelView definitions for all DB models.

All views are read-only by default (no create/edit/delete) because every
record in this system is produced by LiveKit webhooks or Celery workers —
manual edits would break referential integrity.

ComplianceReport is also read-only and shows violation evidence inline.
"""

from sqladmin import ModelView

from app.db.models import (
    ComplianceReport,
    EgressJob,
    Participant,
    Recording,
    Session,
    WebhookEvent,
)


class SessionAdmin(ModelView, model=Session):
    name        = "Session"
    name_plural = "Sessions"
    icon        = "fa-solid fa-video"

    # List view
    column_list = [
        Session.id,
        Session.room_name,
        Session.status,
        Session.composite_s3_url,
        Session.started_at,
        Session.ended_at,
    ]
    column_sortable_list   = [Session.id, Session.started_at, Session.status]
    column_searchable_list = [Session.room_name]
    column_filters         = [Session.status]

    column_details_list = [
        Session.id,
        Session.room_name,
        Session.status,
        Session.composite_s3_url,
        Session.started_at,
        Session.ended_at,
        Session.participants,
        Session.egress_jobs,
    ]

    # Read-only — records are created by webhook handlers
    can_create = False
    can_edit   = False
    can_delete = False


class ParticipantAdmin(ModelView, model=Participant):
    name        = "Participant"
    name_plural = "Participants"
    icon        = "fa-solid fa-users"

    column_list = [
        Participant.id,
        Participant.identity,
        Participant.role,
        Participant.session_id,
        Participant.track_s3_url,
        Participant.joined_at,
        Participant.left_at,
    ]
    column_sortable_list   = [Participant.id, Participant.joined_at, Participant.role]
    column_searchable_list = [Participant.identity]
    column_filters         = [Participant.role]

    column_details_list = [
        Participant.id,
        Participant.session,
        Participant.identity,
        Participant.role,
        Participant.track_s3_url,
        Participant.joined_at,
        Participant.left_at,
    ]

    can_create = False
    can_edit   = False
    can_delete = False


class EgressJobAdmin(ModelView, model=EgressJob):
    name        = "Egress Job"
    name_plural = "Egress Jobs"
    icon        = "fa-solid fa-circle-dot"

    column_list = [
        EgressJob.id,
        EgressJob.egress_id,
        EgressJob.egress_type,
        EgressJob.status,
        EgressJob.identity,
        EgressJob.started_at,
        EgressJob.ended_at,
    ]
    column_sortable_list   = [EgressJob.id, EgressJob.started_at, EgressJob.status, EgressJob.egress_type]
    column_searchable_list = [EgressJob.egress_id, EgressJob.identity]
    column_filters         = [EgressJob.egress_type, EgressJob.status]

    column_details_list = [
        EgressJob.id,
        EgressJob.session,
        EgressJob.egress_id,
        EgressJob.egress_type,
        EgressJob.track_sid,
        EgressJob.identity,
        EgressJob.status,
        EgressJob.started_at,
        EgressJob.ended_at,
        EgressJob.recordings,
    ]

    can_create = False
    can_edit   = False
    can_delete = False


class RecordingAdmin(ModelView, model=Recording):
    name        = "Recording"
    name_plural = "Recordings"
    icon        = "fa-solid fa-file-video"

    column_list = [
        Recording.id,
        Recording.egress_job_id,
        Recording.file_type,
        Recording.s3_key,
        Recording.created_at,
    ]
    column_sortable_list   = [Recording.id, Recording.created_at, Recording.file_type]
    column_searchable_list = [Recording.s3_key]
    column_filters         = [Recording.file_type]

    column_details_list = [
        Recording.id,
        Recording.egress_job,
        Recording.s3_key,
        Recording.file_type,
        Recording.created_at,
    ]

    can_create = False
    can_edit   = False
    can_delete = False


class ComplianceReportAdmin(ModelView, model=ComplianceReport):
    name        = "Compliance Report"
    name_plural = "Compliance Reports"
    icon        = "fa-solid fa-shield-halved"

    column_list = [
        ComplianceReport.id,
        ComplianceReport.participant_identity,
        ComplianceReport.status,
        ComplianceReport.max_faces_detected,
        ComplianceReport.frames_analyzed,
        ComplianceReport.s3_url,
        ComplianceReport.created_at,
        ComplianceReport.processed_at,
    ]
    column_sortable_list   = [ComplianceReport.id, ComplianceReport.created_at, ComplianceReport.status]
    column_searchable_list = [ComplianceReport.participant_identity, ComplianceReport.s3_url]
    column_filters         = [ComplianceReport.status]

    column_details_list = [
        ComplianceReport.id,
        ComplianceReport.egress_job,
        ComplianceReport.participant_identity,
        ComplianceReport.s3_url,
        ComplianceReport.status,
        ComplianceReport.expected_face_count,
        ComplianceReport.max_faces_detected,
        ComplianceReport.frames_analyzed,
        ComplianceReport.violation_frames,
        ComplianceReport.created_at,
        ComplianceReport.processed_at,
        ComplianceReport.error_detail,
    ]

    can_create = False
    can_edit   = False
    can_delete = False


class WebhookEventAdmin(ModelView, model=WebhookEvent):
    name        = "Webhook Event"
    name_plural = "Webhook Events"
    icon        = "fa-solid fa-bolt"

    column_list = [
        WebhookEvent.id,
        WebhookEvent.event_type,
        WebhookEvent.room_name,
        WebhookEvent.received_at,
    ]
    column_sortable_list   = [WebhookEvent.id, WebhookEvent.received_at, WebhookEvent.event_type]
    column_searchable_list = [WebhookEvent.room_name, WebhookEvent.event_type]
    column_filters         = [WebhookEvent.event_type]

    column_details_list = [
        WebhookEvent.id,
        WebhookEvent.event_type,
        WebhookEvent.room_name,
        WebhookEvent.payload,    # full JSON payload
        WebhookEvent.received_at,
    ]

    can_create = False
    can_edit   = False
    can_delete = False
