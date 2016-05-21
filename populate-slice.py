#!/usr/bin/env python
# -*- mode: python; coding: utf-8; fill-column: 80; -*-
#
# populate-slice.py
# Created by Balakrishnan Chandrasekaran on 2016-05-20 19:30 -0400.
# Copyright (c) 2016 Balakrishnan Chandrasekaran <balac@cs.duke.edu>.
#

"""
populate-slice.py
Populates slices with two nodes (maximum) from each site. The nodes are
selected such that the last contact time of the node is no later than a given
threshold.

$ ./populate-slice.py -h
usage: populate-slice.py [-h] [--version] -u usr -s slice-name -d staleness
                         [-n nps]

Renew a PlanetLab slice as far into the future as permitted.

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -u usr, --usr usr     User ID.
  -s slice-name, --slice slice-name
                        Name of an existing PlanetLab slice.
  -d staleness, --days staleness
                        How stale (in days) the "last contact" time of a node
                        can be?
  -n nps, --nps nps     Number of nodes selected per site
"""

__author__  = 'Balakrishnan Chandrasekaran <balac@cs.duke.edu>'
__version__ = '1.0'
__license__ = 'MIT'


import argparse
from collections import defaultdict as defdict
from datetime import datetime as dt, timedelta as td
import plc
import random
import sys


# Default number of nodes to select from each site.
NUM_NPS = 2


def main(args):
    try:
        api = plc.PLC(args.usr, plc.get_pwd(args.usr))

        live_nodes = api.get_live_nodes()
        print("- #live-nodes: %d" % (len(live_nodes)))

        # Time after which if a node was observed, it is considered active.
        cutoff = dt.today() - td(days=args.staleness)
        active_nodes = [n for n in live_nodes if n.was_seen_after(cutoff)]
        print("- #active-nodes: %d" % (len(active_nodes)))

        site_grps = defdict(lambda : [])
        for n in active_nodes:
            site_grps[n.site].append(n)
        print("- #active-sites: %d" % (len(site_grps)))

        sel_nodes = []
        for site in site_grps:
            nodes = site_grps[site]

            if len(nodes) > args.nps:
                random.shuffle(nodes)

            sel_nodes.extend([n.node_id for n in nodes[:args.nps]])
        print("- #selected-nodes: %d" % (len(sel_nodes)))

        reqd_sz = len(sel_nodes)
        s = api.get_slice(args.slice_name)
        print("- slice '%s(%d)' has '%d' nodes" %
              (s.name, s.slice_id, len(s.nodes)))
        print("- slice '%s(%d)' can host '%d' nodes" %
              (s.name, s.slice_id, s.max_nodes))

        if s.max_nodes < reqd_sz:
            raise ValueError(u"Slice size is smaller than required!")

        s = api.add_nodes(s.slice_id, sel_nodes)
        print("- slice '%s(%d)' has '%d' nodes" %
              (s.name, s.slice_id, len(s.nodes)))
    except Exception as e:
        sys.stderr.write(u"Error> %s\n" % (e.message))


def __get_parser():
    """Configure a parser to parse command-line arguments.
    """
    desc = ("Renew a PlanetLab slice as far into the future as permitted.")
    parser = argparse.ArgumentParser(description = desc)
    parser.add_argument('--version',
                        action = 'version',
                        version = '%(prog)s ' + "%s" % (__version__))
    parser.add_argument('-u', '--usr',
                        dest = 'usr',
                        metavar = 'usr',
                        required = True,
                        help = 'User ID.')
    parser.add_argument('-s', '--slice',
                        dest = 'slice_name',
                        required = True,
                        metavar = 'slice-name',
                        help = 'Name of an existing PlanetLab slice.')
    parser.add_argument('-d', '--days',
                        type = int,
                        dest = 'staleness',
                        required = True,
                        metavar = 'staleness',
                        help = ('How stale (in days) the "last contact" time'
                                ' of a node can be?'))
    parser.add_argument('-n', '--nps',
                        type = int,
                        dest = 'nps',
                        metavar = 'nps',
                        default = NUM_NPS,
                        help = 'Number of nodes selected per site')
    return parser


if __name__ == '__main__':
    args = __get_parser().parse_args()
    main(args)
