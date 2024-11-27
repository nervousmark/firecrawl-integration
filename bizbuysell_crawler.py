import requests
import pandas as pd
import os
from dotenv import load_dotenv
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_job_completion(job_id, headers, max_attempts=30, delay=2):
    """
    Poll the job status until it completes or reaches max attempts.
    
    Args:
        job_id (str): The ID of the crawl job
        headers (dict): Request headers containing API key
        max_attempts (int): Maximum number of polling attempts
        delay (int): Delay between polling attempts in seconds
    
    Returns:
        dict: Job data if successful, None if failed or timeout
    """
    for attempt in range(max_attempts):
        status_response = requests.get(
            f"https://api.firecrawl.dev/v0/crawl/status/{job_id}",
            headers=headers
        )
        
        if status_response.status_code == 200:
            status_data = status_response.json()
            current_status = status_data.get("status")
            
            if current_status == "completed":
                logger.info("Job completed successfully")
                return status_data
            elif current_status == "failed":
                logger.error("Job failed")
                return None
            else:
                logger.info(f"Job status: {current_status}. Waiting {delay} seconds...")
                time.sleep(delay)
        else:
            logger.error(f"Error checking job status: {status_response.status_code}")
            logger.error(f"Response: {status_response.text}")
            return None
    
    logger.error(f"Job timed out after {max_attempts} attempts")
    return None

def crawl_bizbuysell():
    # Load environment variables from .env file
    load_dotenv()

    # Retrieve the API key from environment variables
    api_key = os.getenv("FIREFLOW_API_KEY")
    
    if not api_key:
        logger.error("API key not found. Please set the FIREFLOW_API_KEY environment variable.")
        return None
    else:
        logger.info("API key retrieved successfully.")

    url_to_scrape = "https://www.bizbuysell.com/Business-Opportunity/Bathroom-and-Kitchen-Wholesale-and-Retail-Distributor-for-sale/2307073/"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "url": url_to_scrape,
        "crawlerOptions": {
            "mode": "llm-extraction",
            "extractionPrompt": (
                "Extract the company description (in one sentence explain what the company does), "
                "company industry (software, services, AI, etc.) - this really should just be a tag with "
                "a couple keywords, and who they serve (who are their customers). If there is no clear "
                "information to answer the question, write 'no info'."
            ),
            "extractionSchema": {
                "type": "object",
                "properties": {
                    "company_description": {
                        "type": "string"
                    },
                    "company_industry": {
                        "type": "string"
                    },
                    "who_they_serve": {
                        "type": "string"
                    }
                },
                "required": [
                    "company_description",
                    "company_industry",
                    "who_they_serve"
                ]
            }
        }
    }

    try:
        # Submit the crawl job
        response = requests.post("https://api.firecrawl.dev/v0/crawl", headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            if "jobId" in data:
                job_id = data["jobId"]
                logger.info(f"Crawl job submitted successfully. Job ID: {job_id}")
                
                # Wait for job completion
                completed_data = wait_for_job_completion(job_id, headers)
                
                if completed_data:
                    # Process the completed data
                    crawl_data = completed_data.get("data", [{}])[0]
                    df = pd.DataFrame([{
                        "company_description": crawl_data.get("metadata", {}).get("description", "no info"),
                        "company_industry": "no info",  # You might want to extract this from the content
                        "who_they_serve": "no info"  # You might want to extract this from the content
                    }])
                    df.to_csv("bizbuysell_listings.csv", index=False)
                    logger.info("Data saved to bizbuysell_listings.csv")
                    return df
            else:
                logger.error("No jobId in response")
                return None
        else:
            logger.error(f"An error occurred: Status code {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None

    except Exception as e:
        logger.exception(f"An exception occurred: {str(e)}")
        return None

if __name__ == "__main__":
    df = crawl_bizbuysell()