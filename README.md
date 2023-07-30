# Spotfiy Data Pipeline End-To-End Python Data Engineering Project
Implement A Complete Data Pipeline using Python, AWS and Spotipy API

## Introduction
Build an end to end data pipeline to extract data from spotify api, store it in AWS S3, pre-process and transform the data using Python/AWS Lamda functions and display/analyze the data using AWS Glue and Athena.

## Dataset/API Used
This API contains all the information about the music, albumns, artits and more [Spotipy API](https://developer.spotify.com/)

## Technologies/Services Used
1. Python
2. S3 (Simple Storage Service)
3. AWS Lambda
4. CloudWatch
5. Glue Crawler
6. Data Catalog
8. Amazon Athena

## Required/Used Libraries
1. **pandas** for the data transformations.
2. **numpy** for the calculations.
3. **requests** to interact with **spotipy** api. 
4. **spotipy** spotify's API.
5. **boto3** to interact with AWS S3.

## Project Details
Integrating with Spotify API and extracting data.
Deploying code on AWS Lambda for data extraction.
Adding trigger to run the extraction job automatically.
Writing transformation function.
Building automated trigger on transformation function.
Store files on S3.
Building Analytics Tables on data files using Glue and Athena.

## Data Pipeline Flow
Extract Data Spotipy API -> Lambda Trigger(Every one hour) -> Run Extract Code -> Store Raw Data In S3 -> Trigger Transform Function -> Transform Data and Load Into S3 -> Query Using Athena 
