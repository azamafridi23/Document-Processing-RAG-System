import asyncio
import logging
import os
from datetime import datetime
from typing import Optional
import signal

from app.constants import MAX_EMAIL_LENGTH
from app.services.gmail_manager import GmailManager


class GmailPoller:
    def __init__(self, poll_interval: int = 300):  # Default 5 minutes
        self.poll_interval = poll_interval
        self.gmail_manager = GmailManager()
        self.is_running = False
        self.task: Optional[asyncio.Task] = None

    async def start_polling(self):
        """Start the email polling service"""
        try:
            logging.info("Starting Gmail email polling service...")

            # Authenticate with Gmail
            auth_success = await self.gmail_manager.authenticate()
            if not auth_success:
                logging.error("Failed to authenticate with Gmail. Polling service cannot start.")
                return False

            self.is_running = True
            logging.info(f"Gmail polling service started. Checking emails every {self.poll_interval} seconds.")

            # Start the polling loop
            while self.is_running:
                try:
                    await self._poll_and_process_emails()
                    await asyncio.sleep(self.poll_interval)
                except asyncio.CancelledError:
                    logging.info("Gmail polling service cancelled.")
                    break
                except Exception as e:
                    logging.error(f"Error in polling loop: {e}")
                    await asyncio.sleep(60)  # Wait 1 minute on error before retrying

            return True

        except Exception as e:
            logging.error(f"Failed to start Gmail polling service: {e}")
            return False

    async def stop_polling(self):
        """Stop the email polling service"""
        logging.info("Stopping Gmail polling service...")
        self.is_running = False
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logging.info("Gmail polling service stopped.")

    async def _poll_and_process_emails(self):
        """Poll for new emails and process them"""
        try:
            logging.info(f"Polling for new emails at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Process unread emails
            processed_count = await self.gmail_manager.process_unread_emails(max_emails=MAX_EMAIL_LENGTH)

            if processed_count > 0:
                logging.info(f"Successfully processed {processed_count} emails")
            else:
                logging.debug("No new emails to process")

        except Exception as e:
            logging.error(f"Error polling and processing emails: {e}")

    async def run_forever(self):
        """Run the polling service forever (for standalone mode)"""
        try:
            await self.start_polling()
        except KeyboardInterrupt:
            logging.info("Received interrupt signal, shutting down...")
        finally:
            await self.stop_polling()


# Global poller instance for FastAPI integration
gmail_poller: Optional[GmailPoller] = None


async def start_gmail_polling(poll_interval: int = 300):
    """Start Gmail polling service (for FastAPI integration)"""
    global gmail_poller
    if gmail_poller is None:
        gmail_poller = GmailPoller(poll_interval)
        gmail_poller.task = asyncio.create_task(gmail_poller.start_polling())
    return gmail_poller


async def stop_gmail_polling():
    """Stop Gmail polling service (for FastAPI integration)"""
    global gmail_poller
    if gmail_poller:
        await gmail_poller.stop_polling()
        gmail_poller = None


def get_poller_status():
    """Get the current status of the Gmail poller"""
    global gmail_poller
    if gmail_poller:
        return {
            "is_running": gmail_poller.is_running,
            "poll_interval": gmail_poller.poll_interval,
            "task_running": gmail_poller.task and not gmail_poller.task.done()
        }
    return {"is_running": False, "poll_interval": None, "task_running": False}


# Standalone script for running the poller independently
async def main():
    """Main function for running the Gmail poller as a standalone service"""
    poll_interval = int(os.getenv("GMAIL_POLL_INTERVAL", "300"))  # Default 5 minutes

    poller = GmailPoller(poll_interval)

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logging.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(poller.stop_polling())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await poller.run_forever()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())