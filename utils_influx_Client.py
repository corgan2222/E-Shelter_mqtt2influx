from influxdb import InfluxDBClient
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def influxClient(config,log):

    if config['INFLUXDB_USESLL']:   
        if config['INFLUXDB_NOVERIFY']:
            influxdb_client =  InfluxDBClient(config['INFLUXDB_ADDRESS'], config['INFLUXDB_PORT'], config['INFLUXDB_USER'], config['INFLUXDB_PASSWORD'], config['INFLUXDB_DATABASE'], True, False)
        else:
            influxdb_client =  InfluxDBClient(config['INFLUXDB_ADDRESS'], config['INFLUXDB_PORT'], config['INFLUXDB_USER'], config['INFLUXDB_PASSWORD'], config['INFLUXDB_DATABASE'], True, True)
    else:
        influxdb_client = InfluxDBClient(config['INFLUXDB_ADDRESS'], config['INFLUXDB_PORT'], config['INFLUXDB_USER'], config['INFLUXDB_PASSWORD'], config['INFLUXDB_DATABASE'] )
    
    # if config['INFLUXDB_CREATE']:
    #     print('Deleting database %s'%config['INFLUXDB_DATABASE'])
    #     influxdb_client.drop_database(config['INFLUXDB_DATABASE'])
    #     print('Creating database %s'%config['INFLUXDB_DATABASE'])
    #     influxdb_client.create_database(config['INFLUXDB_DATABASE'])
    #     influxdb_client.switch_user(config['INFLUXDB_USER'], config['INFLUXDB_PASSWORD'])


    return influxdb_client   