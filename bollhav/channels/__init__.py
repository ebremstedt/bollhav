"""Notification channels: where a run's outcome can be delivered. A model's
`Notifications` names one per outcome; `bollhav.model.notifications.Channel`
is the protocol a custom one implements."""

from bollhav.channels.teams import MicrosoftTeams

__all__ = ["MicrosoftTeams"]
