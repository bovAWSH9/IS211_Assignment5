#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Week 5 Data Structures Assignment - Tomasz Lodowski"""

import argparse
import csv
import urllib2


class Queue:
    def __init__(self):
        self.data = []

    def Enqueue(self, item):
        self.data.append(item)

    def Dequeue(self):
        return self.data.pop(0)

    def size(self):
        return len(self.data)


class Request:
    def __init__(self, request_time, file, process_time):
        self.request_time = request_time
        self.file = file
        self.process_time = process_time


class Server:
    timeSoFar = -1

    def __init__(self):
        self.task = None
        self.time_remaining = 0

    def process(self):
        if self.task is not None:
            self.time_remaining = self.time_remaining - 1
            self.timeSoFar += 1
            if self.time_remaining <= 0:
                self.task = None

    def isBusy(self):
        if self.task is not None:
            return True
        else:
            return False

    def start_task(self, new_task):
        self.task = new_task
        self.time_remaining = new_task.process_time
        self.timeSoFar = new_task.request_time


def read_file(file_url):
    file = urllib2.urlopen(file_url)
    csv_file = csv.reader(file)

    result = []
    for line in csv_file:
        result.append(line)

    return result


def simulateOneServer(lines):
    waiting_time = [0 for i in range(len(lines))]
    server = Server()
    requests_queue = Queue()
    for line in lines:
        request = Request(int(line[0]), line[1], int(line[2]))
        requests_queue.Enqueue(request)

    cur_request_idx = 0
    while True:
        if not server.isBusy() and not requests_queue.size() == 0:
            cur_request = requests_queue.Dequeue()
            if server.timeSoFar > cur_request.request_time:
                waiting_time[cur_request_idx] = (server.timeSoFar - cur_request.request_time)
            server.start_task(cur_request)
            cur_request_idx += 1
        elif not server.isBusy() and requests_queue.size() == 0:
            break
        else:
            server.process()

    average_waiting_time = 1.0 * sum(waiting_time) / len(waiting_time)

    print("the average waiting time for a request %.8f secs") % average_waiting_time


def simulateManyServers(lines, servers_number):
    waiting_time = [0 for i in range(len(lines))]
    servers = [Server() for i in range(servers_number)]

    requests_queue = Queue()
    for line in lines:
        request = Request(int(line[0]), line[1], int(line[2]))
        requests_queue.Enqueue(request)

    cur_request_idx = 0

    while True:
        for i in range(servers_number):
            if not servers[i].isBusy() and not requests_queue.size() == 0:
                cur_request = requests_queue.Dequeue()
                if servers[i].timeSoFar > cur_request.request_time:
                    waiting_time[cur_request_idx] = (servers[i].timeSoFar - cur_request.request_time)
                servers[i].start_task(cur_request)
                cur_request_idx += 1

        """checking"""
        cnt = 0
        for i in range(servers_number):
            if not servers[i].isBusy() and requests_queue.size() == 0:
                cnt += 1

        if cnt == len(servers):
            break
        else:
            for i in range(servers_number):
                servers[i].process()

    average_waiting_time = 1.0 * sum(waiting_time) / len(waiting_time)
    print("the average waiting time for a request %.8f secs when using %d servers") % (
        average_waiting_time, servers_number)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--file', help='Enter the url of the file')
    parser.add_argument('-c', '--servers', help='Enter number of servers')
    args = parser.parse_args()

    try:
        lines = read_file(args.file)
        if args.file and not args.servers:
            simulateOneServer(lines)
        elif args.file and args.servers:
            simulateManyServers(lines, int(args.servers))
        else:
            print ("Invalid attempt, please enter a url")

    except urllib2.URLError as url_err:
        print("The URL not correct")
        raise url_err


if __name__ == '__main__':
    main()
