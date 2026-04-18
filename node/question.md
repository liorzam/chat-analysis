# Chat Analytics Aggregator

## Problem
Build a program that reads chat data and calculates messaging statistics considering both senders and receivers.

## Input CSVs

**messages.csv**: `message_id,channel_id,user_id,timestamp`

**users.csv**: `user_id,org_id,username`

**channel_participants.csv**: `channel_id,user_id`
- Lists all participants in each channel
- A message sent in a channel is received by all other participants in that channel
- Most channels have users from one org, but ~10% have cross-org participants

## Task
Calculate and print:
1. User who sent the most messages
2. User who received the most messages
3. Organization that sent the most messages
4. Organization that received the most messages

Note: When user A sends a message in a channel with 5 participants, user A sent 1 message and the other 4 users each received 1 message.
