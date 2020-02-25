"""
Objects for pulling and loading data from Postgres. Includes:
    1. Eventbrite: The class for making calls to the Eventbrite API
    2. EventbriteLoader: The class for making Eventbrite calls, transforming
        it appropriately and loading it into the Postgres database
"""
from copy import deepcopy
import datetime
import json
import os
import time
import warnings
import urllib

import arrow
import requests

from database import Database

EVENTBRITE_OAUTH = os.environ['EVENTBRITE_OAUTH']
EVENTBRITE_ORG = os.environ['EVENTBRITE_ORG']

class Eventbrite(object):
    """ Makes Eventbrite REST calls using an OAUTH token """
    def __init__(self):
        self.token = EVENTBRITE_OAUTH
        self.url = 'https://www.eventbriteapi.com/v3'

    def get_token_info(self):
        """ Returns metadata about the account associated with the token """
        params = urllib.parse.urlencode({'token': self.token})
        url = self.url + '/users/me/?' + params
        response = requests.get(url)
        return response

    def get_events(self, org_id, start=None, last_modified=None, page=1):
        """ Pulls a list of events based on id """
        url = self.url + '/organizers/%s/events/'%(org_id)

        # Add the query parameters
        param_dict = {'token': self.token, 'page': page}
        if start:
            date = start + 'T0:00:00'
            param_dict['start_date.range_start'] = date
        if last_modified:
            date = last_modified + 'T0:00:00'
            param_dict['date_modified.range_start'] = date
        params = urllib.parse.urlencode(param_dict)
        url += '?' + params

        # Make and parse the request
        response = requests.get(url)
        if response.status_code != 200:
            code = response.status_code
            msg = 'Response had status code: %s'%(code)
            print(msg)
            return None
        else:
            events = json.loads(response.text)
            return events

    def get_event(self, event_id, page=1):
        """ Returns an event based on an id """
        params = urllib.parse.urlencode({
            'token': self.token,
            'page': page
        })
        url = self.url + '/events/%s'%(event_id)
        url += '?' + params
        response = requests.get(url)
        if response.status_code != 200:
            code = response.status_code
            msg = 'Response had status code: %s'%(code)
            print(msg)
            return None
        else:
            event = json.loads(response.text)
            return event

    def get_attendees(self, event_id, page=1):
        """ Returns the attendees of an event based on an id """
        params = urllib.parse.urlencode({
            'token': self.token,
            'page': page
        })
        url = self.url + '/events/%s/attendees'%(event_id)
        url += '?' + params
        response = requests.get(url)
        if response.status_code != 200:
            code = response.status_code
            msg = 'Response had status code: %s'%(code)
            print(msg)
            return None
        else:
            attendees = json.loads(response.text)
            return attendees

    def get_order(self, order_id, page=1):
        """ Returns metadata about an order """
        params = urllib.parse.urlencode({
            'token': self.token,
            'page': page
        })
        url = self.url + '/orders/%s'%(order_id)
        url += '?' + params
        response = requests.get(url)
        if response.status_code != 200:
            code = response.status_code
            msg = 'Response had status code: %s'%(code)
            print(msg)
            return None
        else:
            order = json.loads(response.text)
            return order

    def get_venue(self, venue_id, page=1):
        """ Returns the metadata for a venue """
        params = urllib.parse.urlencode({
            'token': self.token,
            'page': page
        })
        url = self.url + '/venues/%s'%(venue_id)
        url += '?' + params
        response = requests.get(url)
        if response.status_code != 200:
            code = response.status_code
            msg = 'Response had status code: %s'%(code)
            print(msg)
            return None
        else:
            venue = json.loads(response.text)
            return venue

class EventbriteLoader:
    """Loads data from Eventbrite into Postgres """
    def __init__(self):
        self.database = Database()
        self.eventbrite = Eventbrite()
        self.eventbrite_org = EVENTBRITE_ORG

    def run(self):
        """ Runs the data load process """
        last_load_date = self.database.last_event_load_date()
        if last_load_date:
            one_day_ago = datetime.datetime.now() - datetime.timedelta(days=1)
            first_event = min(one_day_ago, last_load_date)
            start = str(first_event)[:10]
            print('Loading events starting at %s'%(start))
        else:
            print('Loading events from the first available event')
            start = None
        events = self.get_events(start=start, page=1)

        num_events = events['pagination']['object_count']
        if num_events > 0:
            print('There are %s events to process'%(num_events))
        else:
            print('There are not next events. Exiting')
            return

        more_events = True
        while more_events:
            for event in events['events']:
                if not event:
                    continue
                msg = "Loading information for %s"%(event['name']['text'])
                print(msg)
                # Load the event into the database. Delete the current
                # entry in order to maintain the unique index
                event_id = event['id']
                self.database.delete_item('events', event_id)
                self.load_event(event)

                # Load the venue, if it does not already
                # appear in the database
                venue_id = event['venue_id']
                venue_ = self.database.get_item('venues', venue_id)
                if venue_id and not venue_:
                    venue = self.get_venue(venue_id)
                    self.load_venue(venue)

                attendees = self.get_attendees(event_id, page=1)
                more_attendees = True
                while more_attendees:
                    if not attendees:
                        break
                    for attendee in attendees['attendees']:
                        if not attendee:
                            continue
                        self.database.delete_item(
                            'attendees',
                            attendee['id'],
                            {'event_id': event_id}
                        )
                        self.load_attendee(attendee)

                    if not attendees['pagination']['has_more_items']:
                        more_attendees = False
                        break
                    else:
                        page = attendees['pagination']['page_number'] + 1
                        attendees = self.get_attendees(event_id, page)
                # Sleep to avoid the Eventbrite rate limit
                time.sleep(60)

            if not events['pagination']['has_more_items']:
                more_events = False
                break
            else:
                page = events['pagination']['page_number'] + 1
                msg = 'Pulling events on page %s'%(page)
                print(msg)
                events = self.get_events(start, page)
        self.database.refresh_views()

    def get_events(self, start, page=1):
        """
        Pulls events from eventbrite and sleeps if the rate limit
        has been exceeded
        """
        org_id = self.eventbrite_org
        events = self.eventbrite.get_events(org_id=org_id, start=start,
                                            page=page)
        if not events:
            # Sleep until eventbrite resets
            print('Rate limit exceed. Sleeping 30 mins')
            time.sleep(3600)
            events = self.eventbrite.get_events(start=start, page=page)
        return events

    def get_attendees(self, event_id, page=1):
        """
        Pulls attendees from eventbrite and sleeps if the rate limit
        has been exceeded
        """
        attendees = self.eventbrite.get_attendees(event_id, page)
        if not attendees:
            # If events comes back as none, sleep until the
            # Eventbrite rate limit resets
            print('Rate limit exceed. Sleeping 30 mins')
            time.sleep(3600)
            attendees = self.eventbrite.get_attendees(event_id, page)
        return attendees

    def get_venue(self, venue_id, page=1):
        """
        Pull a venue and sleeps if the rate limit
        has been exceeded
        """
        venue = self.eventbrite.get_venue(venue_id, page)
        if not venue:
            print('Rate limit exceed. Sleeping 30 mins')
            time.sleep(3600)
            venue = self.eventbrite.get_venue(event_id, page)
        return venue

    def load_event(self, event):
        """ Loads an event into the database """
        event_ = deepcopy(event)

        start = arrow.get(event_['start']['utc']).datetime
        event_['start_datetime'] = start

        end = arrow.get(event_['end']['utc']).datetime
        event_['end_datetime'] = end

        description = event_['description']['text']
        event_['description'] = description

        name = event_['name']['text']
        event_['name'] = name

        event_['load_datetime'] = datetime.datetime.utcnow()
        self.database.load_item(event_, 'events')

    def load_attendee(self, attendee):
        """ Loads an attendee into the database """
        attendee_ = deepcopy(attendee)

        profile = attendee_['profile']
        if 'name' in profile:
            attendee_['name'] = profile['name']
        if 'first_name' in profile:
            attendee_['first_name'] = profile['first_name']
        if 'last_name' in profile:
            attendee_['last_name'] = profile['last_name']
        if 'email' in profile:
            attendee_['email'] = profile['email']

        cost = attendee_['costs']['gross']['major_value']
        attendee_['cost'] = float(cost)

        attendee_['load_datetime'] = datetime.datetime.utcnow()
        self.database.load_item(attendee_, 'attendees')

    def load_order(self, order):
        """ Loads an order into the database """
        order_ = deepcopy(order)

        cost = order_['costs']['gross']['major_value']
        order_['cost'] = float(cost)

        order_['load_datetime'] = datetime.datetime.utcnow()
        self.database.load_item(order_, 'orders')

    def load_venue(self, venue):
        """ Loads a venue into the database """
        venue_ = deepcopy(venue)

        for key in venue_['address']:
            val = venue_['address'][key]
            venue_[key] = val

        venue_['latitude'] = float(venue_['latitude'])
        venue_['longitude'] = float(venue_['longitude'])
        self.database.load_item(venue_, 'venues')
