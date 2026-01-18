# Antaria Casino Telegram Bot

## Overview
A Telegram casino bot with various games including blackjack, roulette, and PvP gambling features. The bot uses PostgreSQL for persistent data storage including user balances, transactions, games history, and global state.

## Project Structure
- `main.py` - Main bot application with all handlers and game logic
- `models.py` - SQLAlchemy database models (User, Game, Transaction, GlobalState)
- `blackjack.py` - Blackjack game implementation
- `predict_handler.py` - Prediction game handler

## Setup Requirements
- Python 3.11
- PostgreSQL database (automatically configured via DATABASE_URL)
- Telegram Bot Token (TELEGRAM_BOT_TOKEN environment variable)

## Running the Bot
The bot runs via the "Telegram Bot" workflow with `python main.py`. It uses polling mode to receive updates from Telegram.

## Database Tables
- `users` - User data (balance, wagered, referrals, achievements)
- `games` - Game history records
- `transactions` - Financial transaction log
- `global_state` - Global settings (house balance, stickers, PvP state)

## Deployment
Configured as a VM deployment since the bot needs to run continuously for Telegram polling.
