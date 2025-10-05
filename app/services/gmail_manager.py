import os
import base64
import email
import logging
import mimetypes
import re
import requests
import asyncio
from typing import List, Dict, Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

from app.services.agent_service import DocumentAgent
from aioredis import from_url, Redis
from app.database.pg_vector import PGVectorManager

# Gmail API scopes
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify',
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid",
]


class GmailManager:
    def __init__(self):
        self.creds = None
        self.service = None
        self.redis_client = None

    def _do_auth_and_build_service(self):
        """Synchronous part of the authentication logic and service building."""
        creds = None
        # Load credentials from token.json if it exists
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)

        # If no valid credentials available, let user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        
        service = build('gmail', 'v1', credentials=creds)
        return creds, service

    async def authenticate(self):
        """Authenticate with Gmail API using OAuth2 in a non-blocking way."""
        try:
            self.creds, self.service = await asyncio.to_thread(self._do_auth_and_build_service)
            
            if self.creds and self.service:
                logging.info("Gmail authentication successful")
                return True
            else:
                logging.error("Gmail authentication failed: Could not get credentials or build service.")
                return False

        except Exception as e:
            logging.error(f"Gmail authentication failed: {e}")
            return False

    async def get_redis_client(self):
        """Get Redis client for LLM agent"""
        if not self.redis_client:
            self.redis_client = from_url(
                os.getenv("REDIS_URL"),
                encoding="utf-8",
                decode_responses=True
            )
        return self.redis_client

    async def get_unread_emails(self, max_results: int = 10) -> List[Dict]:
        """Get unread emails from Gmail"""
        try:
            if not self.service:
                await self.authenticate()

            # Search for unread emails
            results = self.service.users().messages().list(
                userId='me',
                labelIds=['UNREAD'],
                maxResults=max_results
            ).execute()

            messages = results.get('messages', [])
            emails = []

            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()

                email_data = self._parse_email(msg)
                if email_data:
                    emails.append(email_data)

            return emails

        except HttpError as error:
            logging.error(f"Error getting unread emails: {error}")
            return []

    def _parse_email(self, msg) -> Optional[Dict]:
        """Parse email message and extract relevant information"""
        try:
            headers = msg['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
            date = next((h['value'] for h in headers if h['name'] == 'Date'), '')

            # Extract email body
            body = self._get_email_body(msg['payload'])

            return {
                'id': msg['id'],
                'thread_id': msg['threadId'],
                'subject': subject,
                'sender': sender,
                'date': date,
                'body': body,
                'snippet': msg.get('snippet', ''),
                'headers': headers,  # Include all headers for reply threading
            }

        except Exception as e:
            logging.error(f"Error parsing email: {e}")
            return None

    def _get_email_body(self, payload) -> str:
        """Extract email body from payload"""
        try:
            if 'parts' in payload:
                for part in payload['parts']:
                    if part['mimeType'] == 'text/plain':
                        data = part['body']['data']
                        return base64.urlsafe_b64decode(data).decode('utf-8')
                    elif part['mimeType'] == 'text/html':
                        data = part['body']['data']
                        return base64.urlsafe_b64decode(data).decode('utf-8')

            # If no parts, try to get body directly
            if 'body' in payload and 'data' in payload['body']:
                data = payload['body']['data']
                return base64.urlsafe_b64decode(data).decode('utf-8')

            return ""

        except Exception as e:
            logging.error(f"Error extracting email body: {e}")
            return ""

    def _create_html_email_with_images(self, response_text: str, recipient_name: str) -> MIMEMultipart:
        """
        Creates a MIMEMultipart email object with embedded images from markdown.
        It handles both local file paths and remote URLs for images.
        """
        html_content = response_text
        plain_text_content = response_text

        image_pattern = r'!\[(.*?)\]\((.*?)\)'
        matches = list(re.finditer(image_pattern, response_text))

        if not matches:
            message = MIMEMultipart('alternative')
            plain_body = f"""Dear {recipient_name},\n\n{response_text}\n\nBest regards,\nGreen Gro Biological Assistant"""
            html_body = f"""<html><body>
<p>Dear {recipient_name},</p>
<p>{response_text.replace(os.linesep, '<br>')}</p>
<p>Best regards,<br>Green Gro Biological Assistant</p>
</body></html>"""
            message.attach(MIMEText(plain_body, 'plain'))
            message.attach(MIMEText(html_body, 'html'))
            return message

        message = MIMEMultipart('related')
        msg_alternative = MIMEMultipart('alternative')
        message.attach(msg_alternative)

        attached_images = []
        for i, match in enumerate(matches):
            alt_text = match.group(1)
            image_path = match.group(2)
            image_cid = f'image{i}'

            html_content = html_content.replace(match.group(0), f'<img src="cid:{image_cid}" alt="{alt_text}" style="max-width: 100%; height: auto;">')
            plain_text_content = plain_text_content.replace(match.group(0), f'[{alt_text}]')

            try:
                image_data = None
                if image_path.startswith(('http://', 'https://')):
                    response = requests.get(image_path, timeout=10)
                    response.raise_for_status()
                    image_data = response.content
                    mime_type = response.headers.get('Content-Type')
                else:
                    with open(image_path, 'rb') as img_file:
                        image_data = img_file.read()
                    mime_type, _ = mimetypes.guess_type(image_path)

                if image_data and mime_type and mime_type.startswith('image/'):
                    maintype, subtype = mime_type.split('/')
                    img = MIMEImage(image_data, _subtype=subtype)
                    img.add_header('Content-ID', f'<{image_cid}>')
                    attached_images.append(img)
                else:
                    logging.warning(f"Skipping attachment for {image_path} due to invalid MIME type: {mime_type}")
                    html_content = html_content.replace(f'<img src="cid:{image_cid}" alt="{alt_text}" style="max-width: 100%; height: auto;">', f'[Invalid image format: {image_path}]')

            except FileNotFoundError:
                logging.error(f"Image file not found: {image_path}. It will not be attached.")
                html_content = html_content.replace(f'<img src="cid:{image_cid}" alt="{alt_text}" style="max-width: 100%; height: auto;">', f'[Image not found: {image_path}]')
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to download image from {image_path}: {e}")
                html_content = html_content.replace(f'<img src="cid:{image_cid}" alt="{alt_text}" style="max-width: 100%; height: auto;">', f'[Image failed to download: {image_path}]')
            except Exception as e:
                logging.error(f"Error processing image {image_path}: {e}")

        final_plain_text = f"""Dear {recipient_name},\n\n{plain_text_content}\n\nBest regards,\nGreen Gro Biological Assistant"""
        final_html = f"""<html><body>
<p>Dear {recipient_name},</p>
<p>{html_content.replace(os.linesep, '<br>')}</p>
<p>Best regards,<br>Green Gro Biological Assistant</p>
</body></html>"""

        msg_alternative.attach(MIMEText(final_plain_text, 'plain'))
        msg_alternative.attach(MIMEText(final_html, 'html'))

        for img in attached_images:
            message.attach(img)

        return message

    def _remove_bold_formatting(self, text: str) -> str:
        """Remove bold markdown formatting from text"""
        # Remove **text** -> text
        return re.sub(r'\*\*(.*?)\*\*', r'\1', text)

    async def generate_response(self, query: str) -> str:
        """Generate response using Notion vector database"""
        try:
            redis_client = await self.get_redis_client()
            # The agent's generate_response method is synchronous.
            result = DocumentAgent().generate_response(query)
            print(result)
            # await llm._build_prompt(redis_client)
            # await llm._create_agent()

            # result, context = await llm.qa(query)
            result = result.get("output")
            
            # Remove bold formatting from the response for email
            result = self._remove_bold_formatting(result)
            
            print(result)
            return result

        except Exception as e:
            logging.error(f"Error generating response: {e}")
            return "I apologize, but I'm unable to generate a response at the moment. Please try again later."

    async def send_email_reply(self, original_email: Dict, response: str) -> bool:
        """Send email reply in the same thread, with support for embedded images."""
        try:
            if not self.service:
                await self.authenticate()

            # Extract original message headers
            thread_id = original_email.get('thread_id')
            headers = original_email.get('headers', [])
            message_id = None
            for h in headers:
                if h['name'].lower() == 'message-id':
                    message_id = h['value']
                    break

            # Create the email message using the new helper function
            recipient_name = original_email['sender'].split('<')[0].strip()
            message = self._create_html_email_with_images(response, recipient_name)
            
            # Set headers on the message object
            message['to'] = original_email['sender']
            message['subject'] = f"Re: {original_email['subject']}"
            message['from'] = os.getenv("GMAIL_USER", "your-email@gmail.com")
            if message_id:
                message['In-Reply-To'] = message_id
                message['References'] = message_id

            # Encode the message
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')

            # Send the email as a reply in the same thread
            self.service.users().messages().send(
                userId='me',
                body={'raw': raw_message, 'threadId': thread_id}
            ).execute()

            # Mark original email as read
            self.service.users().messages().modify(
                userId='me',
                id=original_email['id'],
                body={'removeLabelIds': ['UNREAD']}
            ).execute()

            logging.info(f"Reply sent to {original_email['sender']} in the same thread")
            return True

        except Exception as e:
            logging.error(f"Error sending email reply: {e}")
            return False

    async def _is_allowed_sender(self, email_data: Dict) -> bool:
        """Check if the email is from an allowed sender for testing"""
        try:
            # Use the correct database manager with a connection pool
            db_manager = PGVectorManager()
            # This method is now synchronous and should not be awaited.
            allowed_senders = db_manager.get_allowed_senders()
            print(f"Allowed senders from DB: {allowed_senders}")

            if not allowed_senders:
                print("No senders in database - allowing all emails")
                return False  # If no restrictions, allow all

            sender_email = email_data.get("sender", "").lower()
            print(f"Checking sender: {sender_email}")

            for allowed_sender in allowed_senders:
                if allowed_sender.strip().lower() in sender_email:
                    print(f"Sender {sender_email} is allowed")
                    return True

            print(f"Sender {sender_email} is NOT allowed")
            return False

        except Exception as e:
            logging.error(f"Error checking allowed sender: {e}")
            return False  # On error, don't allow the email

    async def process_unread_emails(self, max_emails: int = 10):
        """Process all unread emails and send automated replies"""
        try:
            unread_emails = await self.get_unread_emails(max_emails)

            processed_count = 0
            for email_data in unread_emails:
                # Check if email is from allowed sender
                if not await self._is_allowed_sender(email_data):
                    logging.info(f"Skipping email from {email_data['sender']} - not in allowed senders list")
                    continue

                # Extract query from email body
                query = email_data['body'].strip()
                if not query:
                    query = email_data['snippet'].strip()

                if query:
                    # Generate response using Notion vector database
                    response = await self.generate_response(query)

                    # Send email reply
                    success = await self.send_email_reply(email_data, response)

                    if success:
                        logging.info(f"Successfully processed email from {email_data['sender']}")
                        processed_count += 1
                    else:
                        logging.error(f"Failed to process email from {email_data['sender']}")
                else:
                    logging.warning(f"No query found in email from {email_data['sender']}")

            return processed_count

        except Exception as e:
            logging.error(f"Error processing unread emails: {e}")
            return 0