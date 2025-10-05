from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse
import os
import re
from fastapi import APIRouter, Request, Form
from fastapi.responses import Response, JSONResponse
from app.services.agent_service import DocumentAgent
from app.database.pg_vector import PGVectorManager

# You can set these as environment variables or import from your config
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', 'your_account_sid')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', 'your_auth_token')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER', 'your_twilio_number')

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

router = APIRouter()


def _normalize_e164(num: str) -> str:
    return num.strip() if num else ""


def _is_whitelisted_number(num: str) -> bool:
    manager = PGVectorManager()
    allowed = manager.get_allowed_phone_numbers()
    # Parity with email behavior: if no rows configured, the service will not allow any numbers
    if not allowed:
        return False
    n = _normalize_e164(num)
    return n in set(allowed)


@router.post("/twilio/sms")
async def sms_reply(request: Request, Body: str = Form(...), from_number: str = Form(..., alias="From")):
    """
    Webhook endpoint for Twilio to POST incoming SMS messages.
    Verifies the sender number against a whitelist before answering.
    """
    if not _is_whitelisted_number(from_number):
        # Short response for unauthorized numbers
        twiml = create_twiml_response("Your number is not authorized to use this service.")
        return Response(content=twiml, media_type="application/xml")

    agent = DocumentAgent()
    agent_response = agent.generate_response(Body)
    # Extract the main answer string (no markdown/images for SMS)
    raw_response = agent_response.get('output', 'Sorry, I could not process your request.')
    processed_response = convert_markdown_images_to_links(raw_response)
    twiml = create_twiml_response(processed_response)
    return Response(content=twiml, media_type="application/xml")

@router.post("/twilio/test")
async def test_agent_response(request: Request, query: str = Form(...)):
    """
    Testing endpoint that takes a user query, processes it through the agent,
    and converts markdown images to simple links before returning the response.
    """
    try:
        agent = DocumentAgent()
        agent_response = agent.generate_response(query)
        
        # Extract the main response text
        raw_response = agent_response.get('output', 'Sorry, I could not process your request.')
        
        # Convert markdown images to simple links
        processed_response = convert_markdown_images_to_links(raw_response)
        
        return JSONResponse(content={
            "query": query,
            "raw_response": raw_response,
            "processed_response": processed_response
        })
        
    except Exception as e:
        return JSONResponse(
            content={"error": f"Failed to process query: {str(e)}"},
            status_code=500
        )

def convert_markdown_images_to_links(text: str) -> str:
    """
    Convert markdown image syntax ![alt](path) to simple links.
    Example: ![Product Chart](path/to/image.png) -> Product Chart: Here is the url of the image: path/to/image.png
    """
    # Pattern to match markdown images: ![alt text](image path)
    image_pattern = r'!\[(.*?)\]\((.*?)\)'
    
    def replace_image(match):
        alt_text = match.group(1)
        image_path = match.group(2)
        
        # Create a simple link format with descriptive text
        if alt_text:
            return f"{alt_text}: Here is the url of the image: {image_path}"
        else:
            return f"Image: Here is the url of the image: {image_path}"
    
    # Replace all markdown images with simple links
    processed_text = re.sub(image_pattern, replace_image, text)
    
    return processed_text

def send_sms(to_number: str, message: str):
    """
    Send an SMS message using Twilio REST API.
    """
    message = client.messages.create(
        body=message,
        from_=TWILIO_PHONE_NUMBER,
        to=to_number
    )
    return message.sid

def create_twiml_response(reply_text: str) -> str:
    """
    Create a TwiML response for replying to incoming SMS via webhook.
    If the reply is too long, split it into multiple messages (max 1600 chars per message).
    """
    resp = MessagingResponse()
    max_len = 1600  # Twilio's safe max for a single SMS
    parts = [reply_text[i:i+max_len] for i in range(0, len(reply_text), max_len)]
    for part in parts:
        resp.message(part)
    return str(resp)
