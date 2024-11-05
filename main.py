
import logging
logging.basicConfig(filename='script.log', level=logging.DEBUG)
logging.info('Script started')
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from sqlalchemy import create_engine, Table, MetaData, select, and_, insert, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import time
import random
import os
import requests
import sys
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import numpy as np
from io import BytesIO
sys.path.append("../../modules/pandasdb")
sys.path.append("../../modules/getcreds")
sys.path.append("../../modules/cron_monitor")
from pandasdb import DatabaseModule
from creds import getcreds
from cron_monitor import cron_monitor
from dotenv import load_dotenv
load_dotenv()
caller_guid = os.getenv('caller_guid')
cron_name = "KUFU_angpr_fritidsportalen_process"
os.environ['WDM_SSL_VERIFY'] = '0'

def process_dataframe(df, cookies, func, batch_size=50):
    updated_rows = []
    total_rows = len(df)
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(func, row, cookies) for _, row in batch.iterrows()]
            for future in concurrent.futures.as_completed(futures):
                try:
                    updated_rows.append(future.result())
                except Exception as e:
                    print(f"Error processing row: {e}")
    updated_df = pd.DataFrame(updated_rows)
    return updated_df

def bookingLogin(driver=None):
    creds = getcreds(13, 153, caller_guid)
    if driver is None:
        driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()))
    driver.get('https://fritidsportalen-holbaek.kmd.dk/')
    driver.delete_all_cookies()
    login_modal_button = driver.find_element(By.ID, 'loginModal')
    login_modal_button.click()
    time.sleep(3)
    driver.find_element(By.ID, 'UserName').send_keys(creds['Username'])
    driver.find_element(By.ID, 'Password').send_keys(creds['Password'])
    time.sleep(3)
    driver.find_element(By.ID, 'directLogin').click()
    element_present = EC.presence_of_element_located((By.ID, 'home'))
    WebDriverWait(driver, 10).until(element_present)
    time.sleep(10)
    return driver

@cron_monitor(cron_name)
def mainfunc():
    with DatabaseModule() as db:
        driver = bookingLogin()
        findBookingProcessTime(db, driver)
        getNewBookings(db, driver)
        
        driver.quit()
        print("Job done")

def checkBooking(row, cookies):
    bookingnr = row['Id']
    endpoint = f"https://fritidsportalen-holbaek.kmd.dk/Admin/Booking/GetBookingsListAsync?sEcho=6&iColumns=14&sColumns=Id%2CId%2CCustomerName%2CFacilityName%2CFacilityObjectName%2CReOccuringWeekFrequency%2CFromTime%2CFrom%2CTo%2CCreated%2CBookingOccasionStatus%2CBookingOccasionType%2CContactPersonInfo%2CPurchaser&iDisplayStart=0&iDisplayLength=100&mDataProp_0=Id&bSortable_0=false&mDataProp_1=Id&bSortable_1=true&mDataProp_2=CustomerName&bSortable_2=true&mDataProp_3=FacilityName&bSortable_3=true&mDataProp_4=FacilityObjectName&bSortable_4=true&mDataProp_5=ReOccuringWeekFrequency&bSortable_5=true&mDataProp_6=FromTime&bSortable_6=true&mDataProp_7=From&bSortable_7=true&mDataProp_8=To&bSortable_8=true&mDataProp_9=Created&bSortable_9=true&mDataProp_10=BookingOccasionStatus&bSortable_10=false&mDataProp_11=BookingOccasionType&bSortable_11=true&mDataProp_12=ContactPersonInfo&bSortable_12=true&mDataProp_13=Purchaser&bSortable_13=true&iSortCol_0=1&sSortDir_0=desc&iSortingCols=1&BookingOccasionStatus=Bookings&From=05-11-2024&To=05-11-2027&BookingId={bookingnr}&CustomerId=&BookingOccasionType=V%C3%A6lg%20bookingtypen&ShowCancelled=false&FacilityId=&FacilityObjectId=&ContactPerson=&Purchaser=&ShowConflicts=false&_=1730811639813"
    r = requests.get(endpoint, cookies=cookies, verify=False, timeout=600)
    data = r.json()['aaData']['Bookings'][0]['BookingOccasionStatus']
    soup = BeautifulSoup(data, 'html.parser')
    if soup.get_text() != 'Forespørgsel':
        row['responsedate'] = datetime.now()
    return row

def findBookingProcessTime(db, driver):
    tableName = 'KUFU_angpr_fritidsportalen_booking_process_time'
    df = db.db_query("SELECT * FROM [HiT].[dbo].[KUFU_angpr_fritidsportalen_booking_process_time] where responsedate is null;")
    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
    df = process_dataframe(df, cookies_dict, checkBooking)
    df = df[~df['responsedate'].isnull()]
    if len(df.index) > 0:
        df['Created'] = pd.to_datetime(df['Created'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d %H:%M:%S')
        df['responsedate'] = pd.to_datetime(df['responsedate'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d %H:%M:%S')
        db.db_upsert(df, 'dbo', tableName, ['Id'])


def getNewBookings(db, driver):
    tableName = 'KUFU_angpr_fritidsportalen_booking_process_time'
    current_date = datetime.now()
    startDate = current_date.strftime('%d-%m-%Y')
    endDate = (current_date + timedelta(days=365*3)).strftime('%d-%m-%Y')
    print(startDate, endDate)
    url = "https://fritidsportalen-holbaek.kmd.dk/Admin/Booking/GetBookingsListAsync?sEcho=2&iColumns=14&sColumns=Id%2CId%2CCustomerName%2CFacilityName%2CFacilityObjectName%2CReOccuringWeekFrequency%2CFromTime%2CFrom%2CTo%2CCreated%2CBookingOccasionStatus%2CBookingOccasionType%2CContactPersonInfo%2CPurchaser&iDisplayStart=0&iDisplayLength=100&mDataProp_0=Id&bSortable_0=false&mDataProp_1=Id&bSortable_1=true&mDataProp_2=CustomerName&bSortable_2=true&mDataProp_3=FacilityName&bSortable_3=true&mDataProp_4=FacilityObjectName&bSortable_4=true&mDataProp_5=ReOccuringWeekFrequency&bSortable_5=true&mDataProp_6=FromTime&bSortable_6=true&mDataProp_7=From&bSortable_7=true&mDataProp_8=To&bSortable_8=true&mDataProp_9=Created&bSortable_9=true&mDataProp_10=BookingOccasionStatus&bSortable_10=false&mDataProp_11=BookingOccasionType&bSortable_11=true&mDataProp_12=ContactPersonInfo&bSortable_12=true&mDataProp_13=Purchaser&bSortable_13=true&iSortCol_0=1&sSortDir_0=desc&iSortingCols=1&BookingOccasionStatus=BookingRequests&From=31-10-2024&To=31-10-2027&BookingId=&CustomerId=&BookingOccasionType=V%C3%A6lg%20bookingtypen&ShowCancelled=false&FacilityId=&FacilityObjectId=&ContactPerson=&Purchaser=&ShowConflicts=false"
    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
    r = requests.get(url, cookies=cookies_dict, verify=False, timeout=600)
    df = pd.DataFrame(r.json()['aaData']['Bookings'])
    df = df[['Id', 'Created']]
    df['Created'] = pd.to_datetime(df['Created'].str.extract(r'(\d+)').astype(int)[0], unit='ms')
    df['Created'] = pd.to_datetime(df['Created'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d %H:%M:%S')
    db.db_upsert(df, 'dbo', tableName, ['Id'])
 

    


if __name__ == '__main__':
    mainfunc()
    
