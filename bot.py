import os
import io
import json
import asyncio
import logging
from collections import deque
from datetime import datetime
from typing import Optional, Dict, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.constants import ParseMode

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration (hardcoded)
TELEGRAM_BOT_TOKEN = '8493579116:AAF2VDOfLQlazUbB2YagkAbAusstDG7xjlQ'
ADMIN_USER_IDS = [8285783077]
GOOGLE_CREDENTIALS_JSON = '{"type":"service_account","project_id":"drive-uploader-484112","private_key_id":"9e376f7fc6acea01c279f26b5209a8f91abe2a99","private_key":"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDGPkDN5VRfi+0G\\noOdLRbkyzulnyLfCdhUTM6R8hFfTbQ0fYOdUaCF7hYUlEM1e0YRJyngEOOkPFcRy\\nU28lGn1qAQpGpsfMr+jaxPx+SM4UU2yd8SKjvD17gwkus3FpwHpBr64jCOepuvXX\\nNX9v8lXFjMskuNeMYOzfxW1qgcojafNNzBpwGLFfuXCSODhBKUe59+9vFP/fEihC\\nvg0bDRCaCW70ZTfaqYc0cng1WXl2AutEi/ZTtnSpzYmbwjl3THE/u+jxOnbBX9y+\\nIrSZu3h6uLvLfhQHmzaq6kQLPkIq72aT1M2VJS2z/fV0GDCcPMZVogUC4epb/581\\nTRlZogEDAgMBAAECggEATQRgft1Kkzg0ZK/cS/URhzi1mxlysrzXYTdd/vYXfM9Q\\nGtG17afq4ic0W/sUHnM9dsGPMOy/NpoZUOUzN/H7qVQwHtSqqM3qpBuFkLZOQb1L\\nZG0LLRtzs2lsBtW2CQS7Oou7FmdeQSWeXvasxNwcEHyra1/YdfQt8p2r7YXw9yKC\\nHfQzT5p6VMLmnrSdgDPDhoxZ/bbEux+XneL8u0lRVF0lsjUECRMl9LmfIWCw/OPO\\nHGhO9WvFRD/WhmiIdfOyPu/dny8xsVVuVYl7jqMSUO7egx5OuWFE8mzmoCbgC2ze\\nJMBl6AfGNMYh2ntKwMJBel7hLuHlAuFbwFN6DHN/SQKBgQDszqkGHJtwlDY/82VS\\nrMbYioiVXR692EjxKkjLL2Ne4WkOsqLFWRAzjS7KP8bntdcqnUlTPiCS4oDF9fg9\\nD6oI2uHeIzMiCnGBMfQ2Y9JtpLD3pPnsvPPbXY06udCNvULKReVow6kk0eZJMR8+\\nEQrnAaxC98on4A3Vcwn0cuSBzQKBgQDWT3R3+qtLMvY7EjKqKzsdEPiFb1n3AA5k\\nt+2IzX0Q1K8djRdqjVnU6p/U0f26PqZT/cF42QJfyFB3c5DTsFuSOYkRI4EseIVe\\n8kSKsnr8N3UdkOpuLfz33YcYIMJfX6OQ1gIwdbB0/BH0oN7vEHfmdd7brMUXhzZD\\ne6Bgakn+DwKBgFarZG1aNwHDHP/W4zSZC8XhHhAcns24xSmO4kmLNIFbWooIk05L\\nRIS5nXBkWzYe9XshSd7vlBhuUdciKRKgEwleouAcXfCNhfFVCR6G9lMuvZ5FgTtG\\nBLI60R+3Nj9KD5HcdxQa2GMjd3c+RUMAFc9qa1OvQQvLEwUWL4SKMFvZAoGBAIai\\nm/TUEdDIT1CE+jdVuWllQ/vwHIDEFEbKFfwfvDZapzjVbqSN8KWGUKsO0Wuz+CJE\\nBMu66XbrDUI94/pZjO2sH6QYvgoGHSOs58qKniDdZkIQzHYzhfzhuwE5c2MfXk6b\\nY/y1swmLYmxgtC1UFz89RaiUPa2uUDTM8dAgdiOZAoGBAMkWL59tQOPEIEXPpgiz\\ntz/hF+TAKa5+XpbH4oYtfRFVdaobE1Pv8bw6j4oWtgRTDgpBkc7VoqRwlcVyvFJK\\n7pcsxVgzlV2jiyJ0OknZo/eic6/905Ib8cHbLF3xLqAc68z6mkCViBOpyqVif9t+\\n1QncrbPIQgrWx60nV8GopoIL\\n-----END PRIVATE KEY-----\\n","client_email":"telegramdrivebot@drive-uploader-484112.iam.gserviceaccount.com","client_id":"114462441351878653052","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url":"https://www.googleapis.com/robot/v1/metadata/x509/telegramdrivebot%40drive-uploader-484112.iam.gserviceaccount.com","universe_domain":"googleapis.com"}'
GOOGLE_DRIVE_FOLDER_ID = '1mwbQSzvmjq68pOJjemuk02fa7pNux0kO'

# Upload queue
upload_queue = deque()
is_processing = False

# Statistics
stats = {
    'total_uploads': 0,
    'successful_uploads': 0,
    'failed_uploads': 0,
    'total_size_mb': 0.0
}

class GoogleDriveUploader:
    """Handles Google Drive uploads using service account"""
    
    def __init__(self, credentials_json: str):
        self.credentials = service_account.Credentials.from_service_account_info(
            json.loads(credentials_json),
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        self.service = build('drive', 'v3', credentials=self.credentials)
    
    async def upload_file(self, file_stream: io.BytesIO, filename: str, 
                         mime_type: str, folder_id: str = '') -> Dict:
        """Upload file to Google Drive from memory stream"""
        try:
            file_metadata = {'name': filename}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            media = MediaIoBaseUpload(
                file_stream,
                mimetype=mime_type,
                resumable=True,
                chunksize=1024*1024  # 1MB chunks
            )
            
            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            file = await loop.run_in_executor(
                None,
                lambda: self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name, webViewLink, size'
                ).execute()
            )
            
            return {
                'success': True,
                'file_id': file.get('id'),
                'name': file.get('name'),
                'link': file.get('webViewLink'),
                'size': int(file.get('size', 0))
            }
            
        except HttpError as e:
            logger.error(f"Google Drive upload error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return {
                'success': False,
                'error': str(e)
            }

# Initialize Google Drive uploader
gdrive = GoogleDriveUploader(GOOGLE_CREDENTIALS_JSON)

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_USER_IDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user = update.effective_user
    welcome_msg = (
        f"👋 Welcome {user.first_name}!\n\n"
        "📤 I can upload media files from Telegram to Google Drive.\n\n"
        "**Commands:**\n"
        "/upload - Upload media from this chat\n"
        "/bulk - Bulk upload from a channel (admin only)\n"
        "/queue - View upload queue\n"
        "/stats - View upload statistics\n"
        "/help - Show help message\n"
    )
    
    if is_admin(user.id):
        welcome_msg += "\n**Admin Commands:**\n"
        welcome_msg += "/clear_queue - Clear the upload queue\n"
        welcome_msg += "/set_folder <folder_id> - Set Google Drive folder\n"
    
    await update.message.reply_text(welcome_msg, parse_mode=ParseMode.MARKDOWN)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command handler"""
    help_text = (
        "**📚 How to use:**\n\n"
        "1️⃣ Send me any media file (document, photo, video, audio)\n"
        "2️⃣ I'll add it to the upload queue\n"
        "3️⃣ Files are uploaded to Google Drive automatically\n\n"
        "**Bulk Upload:**\n"
        "• Use /bulk to bulk upload from a channel\n"
        "• Forward messages or provide channel info\n\n"
        "**Supported formats:**\n"
        "• Documents (PDF, DOC, ZIP, etc.)\n"
        "• Photos (JPG, PNG, etc.)\n"
        "• Videos (MP4, AVI, etc.)\n"
        "• Audio (MP3, WAV, etc.)\n"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming media files"""
    message = update.message
    user = update.effective_user
    
    # Extract file info based on media type
    file_obj = None
    filename = None
    mime_type = None
    
    if message.document:
        file_obj = message.document
        filename = file_obj.file_name
        mime_type = file_obj.mime_type or 'application/octet-stream'
    elif message.photo:
        file_obj = message.photo[-1]  # Get highest resolution
        filename = f"photo_{file_obj.file_unique_id}.jpg"
        mime_type = 'image/jpeg'
    elif message.video:
        file_obj = message.video
        filename = file_obj.file_name or f"video_{file_obj.file_unique_id}.mp4"
        mime_type = file_obj.mime_type or 'video/mp4'
    elif message.audio:
        file_obj = message.audio
        filename = file_obj.file_name or f"audio_{file_obj.file_unique_id}.mp3"
        mime_type = file_obj.mime_type or 'audio/mpeg'
    elif message.voice:
        file_obj = message.voice
        filename = f"voice_{file_obj.file_unique_id}.ogg"
        mime_type = 'audio/ogg'
    elif message.video_note:
        file_obj = message.video_note
        filename = f"video_note_{file_obj.file_unique_id}.mp4"
        mime_type = 'video/mp4'
    else:
        await message.reply_text("❌ Unsupported media type.")
        return
    
    # Add to queue
    upload_queue.append({
        'file_obj': file_obj,
        'filename': filename,
        'mime_type': mime_type,
        'user_id': user.id,
        'chat_id': message.chat_id,
        'message_id': message.message_id,
        'timestamp': datetime.now()
    })
    
    queue_position = len(upload_queue)
    await message.reply_text(
        f"✅ Added to queue!\n"
        f"📁 File: `{filename}`\n"
        f"📊 Position: {queue_position}\n"
        f"💾 Size: {file_obj.file_size / (1024*1024):.2f} MB",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Start processing if not already running
    if not is_processing:
        asyncio.create_task(process_queue(context))

async def process_queue(context: ContextTypes.DEFAULT_TYPE):
    """Process upload queue"""
    global is_processing
    
    if is_processing:
        return
    
    is_processing = True
    logger.info("Started queue processing")
    
    while upload_queue:
        item = upload_queue.popleft()
        
        try:
            # Notify user
            await context.bot.send_message(
                chat_id=item['chat_id'],
                text=f"⏳ Uploading `{item['filename']}`...",
                parse_mode=ParseMode.MARKDOWN,
                reply_to_message_id=item['message_id']
            )
            
            # Download file to memory
            file = await context.bot.get_file(item['file_obj'].file_id)
            file_stream = io.BytesIO()
            await file.download_to_memory(file_stream)
            file_stream.seek(0)
            
            # Upload to Google Drive
            result = await gdrive.upload_file(
                file_stream,
                item['filename'],
                item['mime_type'],
                GOOGLE_DRIVE_FOLDER_ID
            )
            
            # Update stats
            stats['total_uploads'] += 1
            
            if result['success']:
                stats['successful_uploads'] += 1
                stats['total_size_mb'] += result['size'] / (1024*1024)
                
                await context.bot.send_message(
                    chat_id=item['chat_id'],
                    text=(
                        f"✅ Upload successful!\n\n"
                        f"📁 File: `{result['name']}`\n"
                        f"🔗 [Open in Drive]({result['link']})\n"
                        f"💾 Size: {result['size'] / (1024*1024):.2f} MB"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_to_message_id=item['message_id']
                )
            else:
                stats['failed_uploads'] += 1
                await context.bot.send_message(
                    chat_id=item['chat_id'],
                    text=f"❌ Upload failed: {result['error']}",
                    reply_to_message_id=item['message_id']
                )
            
            # Clear memory
            file_stream.close()
            
        except Exception as e:
            logger.error(f"Error processing queue item: {e}")
            stats['failed_uploads'] += 1
            await context.bot.send_message(
                chat_id=item['chat_id'],
                text=f"❌ Error: {str(e)}",
                reply_to_message_id=item['message_id']
            )
        
        # Small delay between uploads
        await asyncio.sleep(1)
    
    is_processing = False
    logger.info("Queue processing completed")

async def queue_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show queue status"""
    if not upload_queue:
        await update.message.reply_text("📭 Queue is empty!")
        return
    
    queue_text = f"📊 **Upload Queue** ({len(upload_queue)} items)\n\n"
    
    for i, item in enumerate(list(upload_queue)[:10], 1):
        queue_text += f"{i}. `{item['filename']}`\n"
    
    if len(upload_queue) > 10:
        queue_text += f"\n... and {len(upload_queue) - 10} more"
    
    await update.message.reply_text(queue_text, parse_mode=ParseMode.MARKDOWN)

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show upload statistics"""
    success_rate = 0
    if stats['total_uploads'] > 0:
        success_rate = (stats['successful_uploads'] / stats['total_uploads']) * 100
    
    stats_text = (
        "📊 **Upload Statistics**\n\n"
        f"📤 Total uploads: {stats['total_uploads']}\n"
        f"✅ Successful: {stats['successful_uploads']}\n"
        f"❌ Failed: {stats['failed_uploads']}\n"
        f"📈 Success rate: {success_rate:.1f}%\n"
        f"💾 Total uploaded: {stats['total_size_mb']:.2f} MB\n"
        f"📋 Queue length: {len(upload_queue)}"
    )
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

async def bulk_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bulk upload from channel (admin only)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only command!")
        return
    
    await update.message.reply_text(
        "📦 **Bulk Upload Mode**\n\n"
        "Forward me messages from a channel or group, and I'll add all media to the queue.\n\n"
        "Send /done when finished.",
        parse_mode=ParseMode.MARKDOWN
    )
    
    context.user_data['bulk_mode'] = True

async def clear_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear upload queue (admin only)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only command!")
        return
    
    count = len(upload_queue)
    upload_queue.clear()
    await update.message.reply_text(f"🗑️ Cleared {count} items from queue.")

async def set_folder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set Google Drive folder (admin only)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only command!")
        return
    
    if not context.args:
        await update.message.reply_text(
            "Usage: /set_folder <folder_id>\n\n"
            "Get folder ID from Google Drive URL"
        )
        return
    
    global GOOGLE_DRIVE_FOLDER_ID
    GOOGLE_DRIVE_FOLDER_ID = context.args[0]
    await update.message.reply_text(f"✅ Set folder ID to: `{GOOGLE_DRIVE_FOLDER_ID}`", parse_mode=ParseMode.MARKDOWN)

def main():
    """Main function to run the bot"""
    # Credentials are now hardcoded, no validation needed
    
    # Create application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("queue", queue_status))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("bulk", bulk_upload))
    application.add_handler(CommandHandler("clear_queue", clear_queue))
    application.add_handler(CommandHandler("set_folder", set_folder))
    
    # Media handlers
    media_filter = (
        filters.Document.ALL | filters.PHOTO | filters.VIDEO | 
        filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE
    )
    application.add_handler(MessageHandler(media_filter, handle_media))
    
    # Start bot
    logger.info("Bot started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
