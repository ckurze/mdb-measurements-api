import argparse
import requests
from pprint import pprint
import pymongo

parser = argparse.ArgumentParser(description='Import Metrics from OpsManager / Atlas API')
parser.add_argument('--opsmanager_baseurl', required=True, help='Base URL of OpsManager / Atlas Cluster, no trailing slash (http://172.16.4.50:8080/api/public/v1.0 / https://cloud.mongodb.com/api/atlas/v1.0)')
parser.add_argument('--opsmanager_username', required=True, help='Username for authentication of API')
parser.add_argument('--opsmanager_apikey', required=True, help='API key for authentication of API')

parser.add_argument('--opsmanager_groupid', required=True, help='The id of the Group / Project in OpsManager / Atlas')
parser.add_argument('--opsmanager_clusterid', required=True, help='The id of the Cluster in OpsManager / Atlas')

parser.add_argument('--target_mongouri', required=True, help='The URI of the target mongodb cluster (mongodb://USER:PASSWORD@server1:port,server2:port,server3.port), no mongodb+srv syntax supported')
parser.add_argument('--target_mongodatabase', required=True, help='Target database to store metadata and metrics')

parser.add_argument('--metrics_start', required=True, help='Timestamp when to start with metrics extraction (UTC), e.g. 2019-08-08T07:20:00Z')
parser.add_argument('--metrics_end', required=True, help='Timestamp when to end with metrics extraction (UTC), e.g. 2019-08-08T07:40:00Z')
parser.add_argument('--metrics_granularity', required=True, help='Granularity, e.g. lowest granularity PT10S (10 seconds)')


args = parser.parse_args()

mongo_client = pymongo.MongoClient(args.target_mongouri)
db = mongo_client[args.target_mongodatabase]

def import_metrics():
	print('')
	print('Importing metrics for the following cluster:')
	print('============================================')
	print ('OpsManager:  ' + args.opsmanager_baseurl)
	print ('UserName:    ' + args.opsmanager_username)
	print ('GroupId:     ' + args.opsmanager_groupid)
	print ('ClusterId:   ' + args.opsmanager_clusterid)
	print('')
	print ('Granularity: ' + args.metrics_granularity)
	print ('Start:       ' + args.metrics_start)
	print ('End:         ' + args.metrics_end)
	print('')
	print ('Target DB:   ' + args.target_mongodatabase)
	print ('Target URI:  ' + args.target_mongouri[:args.target_mongouri.find('//')+2] + 'XXXXXXXX:XXXXXXXX' + args.target_mongouri[args.target_mongouri.find('@'):])
	print('')

	cluster_summarized_info = { 
		'groupId': args.opsmanager_groupid, 
		'clusterId': args.opsmanager_clusterid
	}
	cluster_info(cluster_summarized_info)
	cluster_hosts(cluster_summarized_info)
	disk_partitions(cluster_summarized_info)
	databases(cluster_summarized_info)

	all_host_measurements(cluster_summarized_info)
	all_disk_partition_measurements(cluster_summarized_info)
	all_database_measurements(cluster_summarized_info)

	store_cluster_summarized_info(cluster_summarized_info)

	get_metrics_hosts(cluster_summarized_info)
	get_metrics_disk_partitions(cluster_summarized_info)
	get_metrics_database(cluster_summarized_info)

	# pprint(cluster_summarized_info)

def cluster_info(cluster_summarized_info):
	print('Gather information about the cluster...')

	url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/clusters/' + args.opsmanager_clusterid
	headers = { 'Accept': 'application/json' }
	params = { }

	doc = execute_get_call(url, headers, params)
	#pprint(doc)

	try:
		result = db.clusters.update_one(
			{ 'groupId': doc['groupId'], 'id': doc['id']},
			{ '$set': doc },
			upsert=True)
	except pymongo.errors.PyMongoError as e:
		print(e)

	print('    Finished. Stored the results in the target database.')

def cluster_hosts(cluster_summarized_info):
	print('Gather information about the host in the cluster...')

	url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts'
	headers = { 'Accept': 'application/json' }
	params = { 'clusterId': args.opsmanager_clusterid }

	doc = execute_get_call(url, headers, params)
	#pprint(doc)

	cluster_summarized_info['hosts'] = []
	for host in doc['results']:
		cluster_summarized_info['hosts'].append({
			'hostname': host['hostname'],
			'hostId': host['id'],
			'ipAddress': host['ipAddress'],
			'replicaStateName': host['replicaStateName']
		})

	try:
		result = db.cluster_hosts.update_one(
			{ 'groupId': args.opsmanager_groupid, 'clusterId': args.opsmanager_clusterid },
			{ '$set': doc },
			upsert=True)
	except pymongo.errors.PyMongoError as e:
		print(e)

	print('    Finished. Stored the results in the target database.')

def disk_partitions(cluster_summarized_info):
	print('Gather information about the disk partitions of each host in the cluster...')

	for host in cluster_summarized_info['hosts']:
		url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + host['hostId'] + '/disks'
		headers = { 'Accept': 'application/json' }
		params = { }

		doc = execute_get_call(url, headers, params)
		#pprint(doc)

		for summary_host in cluster_summarized_info['hosts']:
			if summary_host['hostId'] == host['hostId']:
				if not 'diskPartitions' in summary_host:
					summary_host['diskPartitions'] = []
				for partition in doc['results']:
					summary_host['diskPartitions'].append(partition['partitionName'])

		try:
			result = db.cluster_host_disk_partitions.update_one(
				{ 'groupId': args.opsmanager_groupid, 'clusterId': args.opsmanager_clusterid, 'hostId': host['hostId'] },
				{ '$set': doc },
				upsert=True)
		except pymongo.errors.PyMongoError as e:
			print(e)

	print('    Finished. Stored the results in the target database.')

def databases(cluster_summarized_info):
	print('Gather information about the databases on each host in the cluster...')

	for host in cluster_summarized_info['hosts']:
		url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + host['hostId'] + '/databases'
		headers = { 'Accept': 'application/json' }
		params = { }

		doc = execute_get_call(url, headers, params)
		#pprint(doc)

		for summary_host in cluster_summarized_info['hosts']:
			if summary_host['hostId'] == host['hostId']:
				if not 'databases' in summary_host:
					summary_host['databases'] = []
				for database in doc['results']:
					summary_host['databases'].append(database['databaseName'])

		try:
			result = db.cluster_host_databases.update_one(
				{ 'groupId': args.opsmanager_groupid, 'clusterId': args.opsmanager_clusterid, 'hostId': host['hostId'] },
				{ '$set': doc },
				upsert=True)
		except pymongo.errors.PyMongoError as e:
			print(e)

	print('    Finished. Stored the results in the target database.')

def all_host_measurements(cluster_summarized_info):
	print('Gather information about all possible metrics for hosts...')

	url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + cluster_summarized_info['hosts'][0]['hostId'] + '/measurements'
	headers = { 'Accept': 'application/json' }
	# special case with PT5M for grnaularity and period -> delivers empty measurements, only a full list of available measurements
	params = { 'granularity': 'PT5M', 'period': 'PT5M' }

	doc = execute_get_call(url, headers, params)
	#pprint(doc)

	cluster_summarized_info['host_measurements'] = []
	for msr in doc['measurements']:
		cluster_summarized_info['host_measurements'].append({
			'name': msr['name'],
			'units': msr['units']
		})

	try:
		result = db.available_measurements_host.update_one(
			{ 'groupId': args.opsmanager_groupid, 'hostId': doc['hostId'] },
			{ '$set': doc },
			upsert=True)
	except pymongo.errors.PyMongoError as e:
		print(e)

	print('    Finished. Stored the results in the target database.')

def all_disk_partition_measurements(cluster_summarized_info):
	print('Gather information about all possible metrics for disk partitions...')

	url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + cluster_summarized_info['hosts'][0]['hostId'] + '/disks/' + cluster_summarized_info['hosts'][0]['diskPartitions'][0] + '/measurements'
	headers = { 'Accept': 'application/json' }
	# special case with PT5M for grnaularity and period -> delivers empty measurements, only a full list of available measurements
	params = { 'granularity': 'PT5M', 'period': 'PT5M' }

	doc = execute_get_call(url, headers, params)
	#pprint(doc)

	cluster_summarized_info['disk_measurements'] = []
	for msr in doc['measurements']:
		cluster_summarized_info['disk_measurements'].append({
			'name': msr['name'],
			'units': msr['units']
		})

	try:
		result = db.available_measurements_disk.update_one(
			{ 'groupId': args.opsmanager_groupid, 'hostId': doc['hostId'], 'partitionName': doc['partitionName'] },
			{ '$set': doc },
			upsert=True)
	except pymongo.errors.PyMongoError as e:
		print(e)

	print('    Finished. Stored the results in the target database.')

def all_database_measurements(cluster_summarized_info):
	print('Gather information about all possible metrics for databases...')

	url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + cluster_summarized_info['hosts'][0]['hostId'] + '/databases/' + cluster_summarized_info['hosts'][0]['databases'][0] + '/measurements'
	headers = { 'Accept': 'application/json' }
	# special case with PT5M for grnaularity and period -> delivers empty measurements, only a full list of available measurements
	params = { 'granularity': 'PT5M', 'period': 'PT5M' }

	doc = execute_get_call(url, headers, params)
	#pprint(doc)

	cluster_summarized_info['database_measurements'] = []
	for msr in doc['measurements']:
		cluster_summarized_info['database_measurements'].append({
			'name': msr['name'],
			'units': msr['units']
		})

	try:
		result = db.available_measurements_database.update_one(
			{ 'groupId': args.opsmanager_groupid, 'hostId': doc['hostId'], 'databaseName': doc['databaseName'] },
			{ '$set': doc },
			upsert=True)
	except pymongo.errors.PyMongoError as e:
		print(e)

	print('    Finished. Stored the results in the target database.')

def store_cluster_summarized_info(cluster_summarized_info):
	print('Store cluster summarized info...')

	try:
		result = db.cluster_summarized_info.update_one(
			{ 'groupId': cluster_summarized_info['groupId'], 'clusterId': cluster_summarized_info['clusterId']},
			{ '$set': cluster_summarized_info },
			upsert=True)
	except pymongo.errors.PyMongoError as e:
		print(e)

	print('    Finished. Stored the results in the target database.')

def get_metrics_hosts(cluster_summarized_info):
	print('Get metrics for hosts...')
	for host in cluster_summarized_info['hosts']:
		# GET /groups/{GROUP-ID}/hosts/{HOST-ID}/measurements
		url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + host['hostId'] + '/measurements'
		headers = { 'Accept': 'application/json' }
		params = { 'granularity': args.metrics_granularity, 'start': args.metrics_start, 'end': args.metrics_end }

		doc = execute_get_call(url, headers, params)
		#pprint(doc)

		try:
			result = db.metrics_hosts.update_one(
				{ 'groupId': args.opsmanager_groupid, 'clusterId': args.opsmanager_clusterid, 'hostId': host['hostId'] },
				{ '$set': doc },
				upsert=True)
		except pymongo.errors.PyMongoError as e:
			print(e)

	print('    Finished. Stored the results in the target database.')

def get_metrics_disk_partitions(cluster_summarized_info):
	print('Get metrics for disk partitions...')
	for host in cluster_summarized_info['hosts']:
		for disk_partition in host['diskPartitions']:
			#GET /groups/{GROUP-ID}/hosts/{HOST-ID}/disks/{PARTITION-NAME}/measurements
			url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + host['hostId'] + '/disks/' + disk_partition + '/measurements'
			headers = { 'Accept': 'application/json' }
			params = { 'granularity': args.metrics_granularity, 'start': args.metrics_start, 'end': args.metrics_end }

			doc = execute_get_call(url, headers, params)
			#pprint(doc)

			try:
				result = db.metrics_disk_partitions.update_one(
					{ 'groupId': args.opsmanager_groupid, 'clusterId': args.opsmanager_clusterid, 'hostId': host['hostId'], 'diskPartition': disk_partition },
					{ '$set': doc },
					upsert=True)
			except pymongo.errors.PyMongoError as e:
				print(e)

	print('    Finished. Stored the results in the target database.')

def get_metrics_database(cluster_summarized_info):
	print('Get metrics for databases...')
	for host in cluster_summarized_info['hosts']:
		for database in host['databases']:
			#GET /groups/{GROUP-ID}/hosts/{HOST-ID}/databases/{DATABASE-NAME}/measurements
			url = args.opsmanager_baseurl + '/groups/' + args.opsmanager_groupid + '/hosts/' + host['hostId'] + '/databases/' + database + '/measurements'
			headers = { 'Accept': 'application/json' }
			params = { 'granularity': args.metrics_granularity, 'start': args.metrics_start, 'end': args.metrics_end }

			doc = execute_get_call(url, headers, params)
			#pprint(doc)

			try:
				result = db.metrics_databases.update_one(
					{ 'groupId': args.opsmanager_groupid, 'clusterId': args.opsmanager_clusterid, 'hostId': host['hostId'], 'databaseName': database },
					{ '$set': doc },
					upsert=True)
			except pymongo.errors.PyMongoError as e:
				print(e)

	print('    Finished. Stored the results in the target database.')

def execute_get_call(url, headers, params):
	response = requests.get(url, headers=headers, params=params, auth=requests.auth.HTTPDigestAuth(args.opsmanager_username, args.opsmanager_apikey))
	if response.status_code == 200: 
		return response.json()
	else:
		print('ERROR: Status Code ' + str(response.status_code) + ' while calling ' + response.url)
		exit()

import_metrics()
