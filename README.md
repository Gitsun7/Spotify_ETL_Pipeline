# Spotfiy Data Pipeline End-To-End Python Data Engineering Project
Implement Complete Data Pipeline using Python, AWS and Spotify API

## Introduction
Building an end to end data pipeline to extract data from spotify api, store it in AWS S3, pre-process and transform the data using Python/AWS Lamda functions and display/analyze the data using AWS Glue and Athena.

## Technologies used
1. Python
2. S3 (Simple Storage Service)
3. AWS Lambda
4. AWS Glue
5. AWS Athena

## Required/Used Libraries
1. **pandas** for the data transformations.
2. **numpy** for the calculations.
3. **requests** to interact with **spotipy** api. 
4. **spotipy** spotify's API.
5. **boto3** to interact with AWS S3.

## Project Details
Integrating with Spotify API and extracting data
Deploying code on AWS Lambda for data extraction
Adding trigger to run the extraction job automatically
Writing transformation function
Building automated trigger on transformation function
Store files on S3.
Building Analytics Tables on data files using Glue and Athena
