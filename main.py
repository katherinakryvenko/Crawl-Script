# encoding = utf8
import MySQLdb
import urllib
import requests
import json
from time import sleep
import csv
import re
from datetime import datetime

# creates a MySQL db for hair salons or hair doctors data
# query string is saved in create_db_salon - for hair salons DB
# and in create_db_doctor - for hair doctors DB
# hair_salon_indicator - if creating hair salons DB it's True, if creating hair doctors DB - False
def create_mysql_db(host, user, password, db, table_name, hair_salons_indicator):
    connection = MySQLdb.connect(host=host, user=user, passwd=password, db=db)
    query = "CREATE TABLE `" + db + "`.`" + table_name
    if hair_salons_indicator is True:
        with open('create_db_salon.txt', 'r') as txt_file:
            text = txt_file.read()
            query += text
    else:
        with open('create_db_doctor.txt', 'r') as txt_file:
            text = txt_file.read()
            query += text
    current = connection.cursor()
    current.execute(query)
    row = current.fetchall()
    connection.commit()


# imports the data from csv to MySQL DB
def csv_to_db(host, user, password, db, table_name, csv_file, hair_salon_indicator):
    connection = MySQLdb.connect(host=host, user=user, passwd=password, db=db)
    connection.set_character_set('utf8')
    current = connection.cursor()
    current.execute('SET NAMES utf8;')
    current.execute('SET CHARACTER SET utf8;')
    current.execute('SET character_set_connection=utf8;')
    connection.commit()
    with open(csv_file, 'r') as infile:
        dict = csv.DictReader(infile, delimiter=str(';'))
        for row in dict:
            if hair_salon_indicator is True:
                query = "INSERT INTO `" + db + "`.`" + table_name + "` (`id`, `business_name`, `address`, `city`, `state`, `country`, `postal_code`, `phone_number`, `website`, `mon_hours`, `tue_hours`, `wed_hours`, `thu_hours`, `fri_hours`, `sat_hours`, `sun_hours`, `rating`, `type`) VALUES ('" + \
                        row['place_id'] + "', '" + row['business_name'] + "', '" + row['address'] + "', '" + row[
                            'city'] + "', '" + row['state'] + "', '" + row['country'] + "', '" + row[
                            'postal_code'] + "', '" + row['phone_number'] + "', '" + row['website'] + "', '" + row[
                            'mon_hours'].decode('utf-8') + "', '" + row['tue_hours'].decode('utf-8') + "', '" + row[
                            'wed_hours'].decode('utf-8') + "', '" + row['thu_hours'].decode('utf-8') + "', '" + row[
                            'fri_hours'].decode('utf-8') + "', '" + row['sat_hours'].decode('utf-8') + "', '" + row[
                            'sun_hours'].decode('utf-8') + "', '" + row['rating'] + "', '" + row['type'] + "');"

            else:
                query = "INSERT INTO `" + db + "`.`" + table_name + "` (`id`, `business_name`, `address`, `city`, `state`, `country`, `postal_code`, `phone_number`, `website`, `mon_hours`, `tue_hours`, `wed_hours`, `thu_hours`, `fri_hours`, `sat_hours`, `sun_hours`, `rating`) VALUES ('" + \
                        row['place_id'] + "', '" + row['business_name'] + "', '" + row['address'] + "', '" + row[
                            'city'] + "', '" + row['state'] + "', '" + row['country'] + "', '" + row[
                            'postal_code'] + "', '" + row['phone_number'] + "', '" + row['website'] + "', '" + row[
                            'mon_hours'].decode('utf-8') + "', '" + row['tue_hours'].decode('utf-8') + "', '" + row[
                            'wed_hours'].decode('utf-8') + "', '" + row['thu_hours'].decode('utf-8') + "', '" + row[
                            'fri_hours'].decode('utf-8') + "', '" + row['sat_hours'].decode('utf-8') + "', '" + row[
                            'sun_hours'].decode('utf-8') + "', '" + row['rating'] + "');"
            try:
                current = connection.cursor()
                current.execute(query)
                row = current.fetchall()
                connection.commit()
            except MySQLdb.IntegrityError:
                print 'This raw is in DB already'


# returns the city list from csv
# each list element is a dictionary (keys: 'city', 'state')
def get_city_list():
    city_list = []
    with open('city_list.csv', 'r') as infile:
        for row in csv.DictReader(infile, delimiter=str(';')):
            city_list.append(row)
    return city_list


# data scraping script, returns a list of dictionaries, each dict contains place data
# has 4 arguments
# query - string google query (for example, 'hair salons in ' or 'hair transplant doctors near ')
# city_list - list of dictionaries which we get using get_city_list() method
# hair_salon_indicator - if sraping hair salons it's True, if sraping hair doctors - False
# api_key - string containing api_key
def scrape_data(query, city_list, hair_salon_indicator, api_key):
    place_list = [] # list of dictionaries, each dict contains data about one place
    session = requests.Session()
    session.headers.update({'User-Agent': 'Google Chrome'})
    address_components = ["street_number", "route", "locality", "administrative_area_level_1", "country", "postal_code"] # using it when scraping address data from json
    for city in city_list:
        city_query = query + city['city'] + ' ' + city['state'] # adding location to a google query
        search_url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query=' + city_query.replace(' ',
                                                                                                   '+') + '&key=' + api_key
        str_file = session.get(search_url)
        json_file = str_file.json()
        while True: # using a loop to manage using next google result page (page_token parameter)
            if json_file['status'] == 'OK':
                places = json_file['results'] # list which incudes up to 20 places
                for place in places: # loop through each place in result list
                    if 'rating' in place:
                        rating = place['rating']
                        if rating >= 3.0: # pulling data only about places with rating above 3.0
                            place_id = place['place_id'] # using place_id to get place details
                            place_url = 'https://maps.googleapis.com/maps/api/place/details/json?placeid=' + place_id + '&key=' + api_key
                            place_str_file = session.get(place_url, )
                            place_json_file = place_str_file.json()
                            if place_json_file['status'] == 'OK':
                                place_info = place_json_file['result']
                                info_dict = {}
                                # then just json parsing
                                info_dict['place_id'] = place_id
                                info_dict['business_name'] = place_info['name'].encode('utf-8')
                                address_list = place_info["address_components"]
                                for component in address_list:
                                    if component['types'][0] in address_components:
                                        if component['types'][0] == "street_number":
                                            info_dict['address'] = component['long_name'].encode('utf-8') + ' '
                                        elif component['types'][0] == "route":
                                            if 'address' in info_dict:
                                                info_dict['address'] += component['long_name'].encode('utf-8')
                                            else:
                                                info_dict['address'] = component['long_name'].encode('utf-8')
                                        elif component['types'][0] == "locality":
                                            info_dict['city'] = component['long_name'].encode('utf-8')
                                        elif component['types'][0] == "administrative_area_level_1":
                                            info_dict['state'] = component['long_name'].encode('utf-8')
                                        elif component['types'][0] == "country":
                                            info_dict['country'] = component['long_name'].encode('utf-8')
                                        elif component['types'][0] == "postal_code":
                                            info_dict['postal_code'] = component['long_name']
                                if 'city' not in info_dict:
                                    for component in address_list:
                                        if component['types'][0] == "neighborhood":
                                            info_dict['city'] = component['long_name'].encode('utf-8')
                                if 'address' not in info_dict:
                                    if 'formatted_address' in place_info:
                                        form_address = place_info['formatted_address']
                                        regex = '(.+?),'
                                        pattern = re.compile(regex)
                                        info_dict['address'] = re.findall(pattern, form_address)[0]
                                if 'international_phone_number' in place_info:
                                    info_dict['phone_number'] = place_info['international_phone_number'].encode('utf-8')
                                else:
                                    info_dict['phone_number'] = ""
                                if 'website' in place_info:
                                    info_dict['website'] = place_info['website']
                                else:
                                    info_dict['website'] = ""
                                info_dict['rating'] = place_info['rating']
                                if 'opening_hours' in place_info:
                                    for day in place_info['opening_hours']['weekday_text']:
                                        regex = "(.+?):"
                                        pattern = re.compile(regex)
                                        day_name = re.findall(pattern, day)
                                        day_name = day_name[0][:3].lower()
                                        regex = ": (.+?)$"
                                        pattern = re.compile(regex)
                                        info_dict[day_name+"_hours"] = str(re.findall(pattern, day)[0].encode('utf-8'))
                                if hair_salon_indicator is True:
                                    if "types" in place_info:
                                        types = place_info['types']
                                        if "beauty_salon" in types and "spa" in types:
                                            info_dict['type'] = "beauty&spa salon"
                                        elif "beauty_salon" in types and "spa" not in types:
                                            info_dict['type'] = "beauty salon"
                                        else:
                                            info_dict['type'] = "hair_salon"
                                place_list.append(info_dict)
                            else:
                                print 'API limit'
                                return place_list
            else:
                print 'API limit'
                return place_list
            if 'next_page_token' in json_file:
                search_url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query=' + query.replace(' ', '+') + '&key=' + api_key + '&pagetoken=' + json_file['next_page_token']
                str_file = session.get(search_url)
                json_file = str_file.json()
                print 'again'
            else:
                break
    return place_list

# imports place list returned by scrape_data() method to csv file
def results_to_csv(place_list, hair_salons, file_name):
    with open(file_name+'.csv', 'w+') as csv_file:
        fieldnames = ['place_id', 'business_name', 'address', 'city', 'state', 'country', 'postal_code', 'phone_number',
                      'website', 'mon_hours', 'tue_hours', 'wed_hours', 'thu_hours', 'fri_hours', 'sat_hours',
                      'sun_hours', 'rating'
                      ]
        if hair_salons is True:
            fieldnames.append('type')
        w = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=str(';'))
        w.writeheader()
        w.writerows(place_list)


