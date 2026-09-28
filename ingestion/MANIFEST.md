# Dataset Manifest

## Dataset Name
chatbot-conversations

## Description
3,725 conversational question-answer pairs used for chatbot training,
sourced from Kaggle. Same dataset used in project DHAP-34.

## Local CSV Path
dags/extraction/3K Conversations Dataset for ChatBot.csv

## Target MinIO Bucket / Path
- **Bucket:** chatbot-conversations
- **Path:** chatbot-conversations/parquet/

## Columns
| Column   | Type    | Nullable |
|----------|---------|----------|
| id       | INTEGER | FALSE    |
| question | TEXT    | FALSE    |
| answer   | TEXT    | FALSE    |

## Source
Kaggle — 3K Conversations Dataset for ChatBot

## Owner
Aadya Anil Kumar

## Last Updated
2026
