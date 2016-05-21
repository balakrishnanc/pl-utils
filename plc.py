#!/usr/bin/env python
# -*- mode: python; coding: utf-8; fill-column: 80; -*-
#
# plc.py
# Created by Balakrishnan Chandrasekaran on 2016-05-20 19:33 -0400.
# Copyright (c) 2016 Balakrishnan Chandrasekaran <balac@cs.duke.edu>.
#

"""
plc.py
Helper module to interact with PlanetLab using XML-RPC.
"""

__author__  = 'Balakrishnan Chandrasekaran <balac@cs.duke.edu>'
__version__ = '1.0'
__license__ = 'MIT'


from collections import namedtuple as nt, defaultdict as defdict
from datetime import datetime as dt, timedelta as td
import getpass
import xmlrpclib as rpc


# URL of the XML-RPC interface to PLC.
PLC_XML_RPC_URL = 'https://www.planet-lab.org/PLCAPI/'

# Limit on how far into the future the PlanetLab slice can be renewed.
RENEWAL_CAP = td(weeks=8)
# Renewal resolution.
RENEWAL_RESOL = td(days=1)

UNIX_EPOCH = dt(1970, 1, 1)


def to_unix_time(ts):
    """Convert timestamp to seconds since UNIX epoch.
    """
    return (ts - UNIX_EPOCH).total_seconds()



def renew_upto(days=RENEWAL_CAP):
    """Get the maximum renewal time in the future.
    """
    return dt.today() + days


SLICE_ATTRIBS = {u'name'      : u'name',
                 u'slice_id'  : u'slice_id',
                 u'expires'   : u'expires',
                 u'site'      : u'site_id',
                 u'max_nodes' : u'max_nodes',
                 u'nodes'     : u'node_ids',
                 u'users'     : u'person_ids',
                 u'desc'      : u'description',
                 u'tags'      : u'slice_tag_ids'}
SliceAttribs = nt(u'SliceAttribs',
                  SLICE_ATTRIBS.keys())(*SLICE_ATTRIBS.values())


class Slice:
    """Slice information.
    """
    __slots__ = SLICE_ATTRIBS.keys()

    def __init__(self, info):
        """Initialize slice with data provided.
        """
        self.name      = info[SliceAttribs.name]
        self.slice_id  = info[SliceAttribs.slice_id]
        self.expires   = dt.utcfromtimestamp(info[SliceAttribs.expires])
        self.site      = info[SliceAttribs.site]
        self.max_nodes = info[SliceAttribs.max_nodes]
        self.nodes     = info[SliceAttribs.nodes]
        self.users     = info[SliceAttribs.users]
        self.desc      = info[SliceAttribs.desc]
        self.tags      = info[SliceAttribs.tags]


    def can_renew(self):
        """Check if the slice can be renewed at this time.
        """
        return (renew_upto() - self.expires) >= RENEWAL_RESOL


class PLC:
    """PlanetLab RPC Interface.
    """

    def __init__(self, usr, pwd):
        """Initialize PLC with authentication credentials.
        """
        self._api = rpc.ServerProxy(PLC_XML_RPC_URL)

        self._auth = {}
        self.__init_auth(usr, pwd)
        self.check_auth()


    def __init_auth(self, usr, pwd):
        """Initialize authentication credentials.
        """
        self._auth['AuthMethod'] = 'password'
        self._auth['Username'] = usr
        self._auth['AuthString'] = pwd


    def check_auth(self):
        """Check authentication credentials.
        """
        try:
            return self._api.AuthCheck(self._auth) == 1
        except rpc.Fault as e:
            if e.faultCode == 103:
                raise ValueError(u'Authentication failed!')
            else:
                raise ValueError(e.faultString)


    def get_slice(self, ident):
        """Retrieve slice information.
        """
        slices = self._api.GetSlices(self._auth, [ident])

        if not slices:
            raise ValueError(u'Failed to retrieve slices!')

        if len(slices) != 1:
            raise ValueError(u'Retrieved more than one slice!')

        return Slice(slices[0])


    def renew_slice(self, sid, ts=None):
        """Renew slice up to the given time.
        """
        if not ts:
            upto = int(to_unix_time(renew_upto()))
        else:
            upto = int(to_unix_time(ts))

        if self._api.UpdateSlice(self._auth, sid, {'expires' : upto}) == 1:
            return self.get_slice(sid)
        raise ValueError(u'Failed to renew slice!')


def get_pwd(usr):
    """Retrieve password associated with the username.
    """
    print("#> Username: %s" % (usr))
    return getpass.getpass("#> Password: ")

