# -*- mode : python; fill-column: 80; -*-
from __future__ import print_function
from disquspy import DisqusAPI, APIError
from json import dumps, load
from sys import stderr
from time import sleep
from argparse import ArgumentParser, ArgumentTypeError
from os import access, R_OK
from os.path import isdir

DESCRIPTION = (
    """This program will attempt to download all Disqus comments from a
    particular forum by iterating through all comments in all
    threads. The output will be stored in files as json objects; one
    per thread. If during this export process an APIError is received
    the program will sleep and try again. In other words, all
    APIErrors are assumed to be raise due to the request/minutes quota
    being exceeded.""")


def readable_dir(values):
    prospective_dir = values
    if not isdir(prospective_dir):
        raise ArgumentTypeError(
            "readable_dir:{0} is not a valid path".format(prospective_dir))
    if access(prospective_dir, R_OK):
        return prospective_dir
    else:
        raise ArgumentTypeError(
            "readable_dir:{0} is not a readable dir".format(prospective_dir))

parser = ArgumentParser(description=DESCRIPTION)

parser.add_argument('-s',
                    '--secret',
                    help='The OAuth API secret provided by Disqus.',
                    required=True,
                    type=str)
parser.add_argument('-k',
                    '--key',
                    help='The OAuth API key provided by Disqus.',
                    required=True,
                    type=str)
parser.add_argument('-f',
                    '--forum',
                    help='The name of the forum that will be exported.',
                    required=True,
                    type=str)
parser.add_argument('-d',
                    '--dir',
                    help='The directory where output will be stored.',
                    required=False,
                    type=readable_dir,
                    default="./")
parser.add_argument('-t',
                    '--sleeptime',
                    help='The seconds to sleep after receiving an APIError.',
                    required=False,
                    type=int,
                    default=300)
args = vars(parser.parse_args())


def warning(*objs):
    """Print a string to standard error prefaced by "WARNING:"
    """
    print("WARNING: ", *objs, file=stderr)


def get(sleeptime, api, endpoint, params):
    """Request information from Disqus."""
    curres = None
    while True:
        try:
            curres = api.get(endpoint, **params)
            return curres
        except APIError as e:
            warning("Received %s while performing get(%s,%s) sleeping for %i"
                    % (e, endpoint, params, sleeptime))
            sleep(sleeptime)


def export(pubkey, prikey, forum, interfaces, directory, sleeptime=300):

    disqus = DisqusAPI(public_key=pubkey,
                       secret_key=prikey,
                       interfaces=interfaces)

    params = {'forum': forum,
              'method': 'GET'}
    threads = get(sleeptime, disqus, 'forums.listThreads', params)
    thread_it = threads.__iter__()
    morethreads = True

    while morethreads:
        try:
            thread = next(thread_it)
            tid = thread['id']
            print("Retrieving posts for '{}'({})".format(
                thread['clean_title'].encode('utf-8'),
                tid))
            params = {'thread': tid, 'method': 'GET'}
            posts = get(sleeptime, disqus, 'posts.list', params)
            post_it = posts.__iter__()
            content = {}
            content['thread'] = thread
            content['posts'] = []
            moreposts = True
            while moreposts:
                try:
                    post = next(post_it)
                    content['posts'].append(post)
                except APIError as e:
                    warning("Received %s while fetching posts sleeping for %i"
                            % (e, sleeptime))
                    sleep(sleeptime)
                except StopIteration:
                    if posts.cursor and posts.cursor['more']:
                        moreposts = posts.cursor['more']
                        params = {'thread': tid,
                                  'method': 'GET',
                                  'cursor': posts.cursor['id']}
                        posts = get(sleeptime,
                                    disqus,
                                    'posts.list',
                                    params)
                        post_it = posts.__iter__()
                    else:
                        break
            content = dumps(content).encode()
            with open('%s/%s.json' % (directory, tid), 'w') as fd:
                fd.write(content)
        except APIError as e:
            warning("Received %s while fetching threads sleeping for %i"
                    % (e, sleeptime))
            sleep(sleeptime)
        except StopIteration:
            if threads.cursor and threads.cursor['more']:
                morethreads = threads.cursor['more']
                params = {'forum': forum,
                          'method': 'GET',
                          'cursor': threads.cursor['id']}
                threads = get(sleeptime,
                              disqus,
                              'forums.listThreads',
                              params)
                thread_it = threads.__iter__()
            else:
                break

with open('interfaces.json') as fp:
    export(args['key'],
           args['secret'],
           args['forum'],
           load(fp), args['dir'],
           args['sleeptime'])
