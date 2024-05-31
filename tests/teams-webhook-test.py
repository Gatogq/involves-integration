from prefect.blocks.notifications import MicrosoftTeamsWebhook
teams_webhook_block = MicrosoftTeamsWebhook.load("teams-notifications-webhook")
teams_webhook_block.notify("Esta es una prueba de notificación :)")