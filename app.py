import logging
import paramiko
from flask import Flask, request
from flask_restplus import Api, Resource, fields
from kafka.admin import KafkaAdminClient, ACLFilter, ACLPermissionType, ResourcePattern, ResourceType, ACL, ACLOperation, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import UnknownTopicOrPartitionError, InvalidReplicationFactorError, IllegalArgumentError, TopicAlreadyExistsError
from functools import wraps

#####################################################################################################################
# Application Configuration
#####################################################################################################################

config = dict( l.rstrip().split('=') for l in open("tools.config") if not l.startswith("#") )
user_db = dict( i.split(':') for i in config['app.valid_users'].split(',') )


#####################################################################################################################
# Application Authentication
#####################################################################################################################
def requires_Auth(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        auth = request.authorization
        if user_db[auth.username] == auth.password :
           return f(*args, **kwargs)
        else:
            return "UNAUTHORIZED !!!!", 401
    return decorator

authorizations = {
    'basicAuth': {
        'type': 'basic',
        'in': 'header',
        'name': 'Authorization'
    }
}

#####################################################################################################################
# Application Defination
#####################################################################################################################

app = Flask(__name__)
api = Api(app = app,
          version = "1.0",
          title = "pykafkaadmintools",
          description = "Python Kafka Cluster Admin Tool",
          authorizations=authorizations)

logging.basicConfig(filename='pykafkaadmintools.log', level=logging.DEBUG)

#####################################################################################################################
# Utility Functions
#####################################################################################################################
def remote_execute(host, service, action, state=None) :

  app.logger.info("Function: remote_execute, Host: {0}, Service: {1}, Action: {2}, State: {3}.".format(host, service, action, state))
  username = config['ssh.user']
  password = config['ssh.password']
  command_config = "ssh.{}.service_script".format(service.lower())

  if 'yes' in config['ssh.issudo'] :
    command = "sudo {}".format(config[command_config])
  else :
    command = config[command_config]

  if action == 'get' :
    command = command + " status"
  else :
    command = command + " " + state.lower()

  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.connect(host, username=username, password=password)
  stdin, stdout, stderr = client.exec_command(command)
  first_line = stdout.readlines()[0]

  if action == "get" :
    string_expected = config["ssh.{}.get_expected_string".format(service.lower())]
    if string_expected.lower() in first_line.lower() :
      return "started"
    else :
      return "stopped"
  else :
    string_expected = config["ssh.{0}.{1}_expected_string".format(service.lower(), state.lower())]
    if string_expected.lower() in first_line.lower() :
      return "success"
    else :
      return "error"
 

#####################################################################################################################
# Topic Management
#####################################################################################################################

ns_topic = api.namespace("topic", description="Manage Kafka Topic")

topic_detail = api.model( 'TopicDetail', {'partitions': fields.Integer(default=1, description="Number of Partitions"), 
                                          'replicas': fields.Integer(default=1, description="Number of Replicas")} ) 

@ns_topic.route("/<string:topicName>")
@ns_topic.doc(params={'topicName': 'Topic Name'})
class Topic(Resource):

  decorators = [requires_Auth]
  @ns_topic.doc(security='basicAuth')
  @ns_topic.marshal_with(topic_detail)
  def get(self, topicName):
    """
    Get Topic Detail.

    """
    app.logger.info("Request to get details for topic {0}.".format(topicName))
    try :
      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      result = admin.describe_topics([topicName])

    except UnknownTopicOrPartitionError as e:
      api.abort(500, e.description)
    except Exception as e:
      api.abort(500, str(e.args))
    finally:
      admin.close()
    app.logger.debug(result)

    if result[0]['error_code'] == 0 :
      return { 'partitions': len(result[0]['partitions']), 'replicas': len(result[0]['partitions'][0]['replicas']) }
    else :
      api.abort(400, "Bad Request(Wrong Topic Name)")


  decorators = [requires_Auth]
  @ns_topic.doc(security='basicAuth')
  @ns_topic.expect(topic_detail)
  def post(self, topicName):
    """
    Create New Topic.

    """
    partitions = request.json['partitions']
    replicas = request.json['replicas']
    app.logger.info("Request to create topic witn name {0} and {1} partitions and {2} replicas.".format(topicName, partitions, replicas))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      newTopic = NewTopic(topicName, partitions, replicas)
      result = admin.create_topics([newTopic]) 

    except TopicAlreadyExistsError as e:
      api.abort(400, e.description)
    except IllegalArgumentError as e:
      api.abort(400, e.description)
    except InvalidReplicationFactorError as e:
      api.abort(400, e.description)
    except Exception as e:
      api.abort(500, str(e.args))
    finally:
      admin.close()

    app.logger.debug(result)

    if result.topic_errors[0][1] == 0 :
      return { "created": topicName }
    else :
      api.abort(400, "Bad Request(Topic Creation Failed)")

    

  decorators = [requires_Auth]
  @ns_topic.doc(security='basicAuth')
  def delete(self, topicName):
    """
    Delete Topic.

    """
    app.logger.info("Request to delete topic witn name {0}.".format(topicName))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      result = admin.delete_topics([topicName])

    except UnknownTopicOrPartitionError as e:
      api.abort(400, e.description)
    except Exception as e:
      api.abort(500, str(e.args))
    finally:
      admin.close()

    app.logger.debug(result)

    if result.topic_error_codes[0][1] == 0 :
      return { "deleted": topicName }
    else :
      api.abort(400, "Bad Request(Topic Deletion Failed)")


class Topics(Resource):
  
  def get(self):
    """
    Get List of Topics.

    """
    app.logger.info("Request to get list of topics.")
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      return admin.list_topics()

    except Exception as e:
      ns_topic.abort(500, str(e.args))
    finally:
      admin.close()

ns_topic.add_resource(Topics, '/listtopics', methods=['GET'])

#####################################################################################################################
# Config Management
#####################################################################################################################

ns_config = api.namespace("config", description="Manage Kafka Topic Configuration")

config_detail = api.model( 'ConfigDetail', {'key': fields.String(description="Configuration Key"), 
                                            'value': fields.String(description="Configuration Value")} ) 

@ns_config.route("/<string:topicName>")
@ns_config.doc(params={'topicName': 'Topic Name'})
class Config(Resource):

  decorators = [requires_Auth]
  @ns_topic.doc(security='basicAuth')
  @ns_topic.marshal_with(config_detail)
  def get(self, topicName):
    """
    Get Topic Configuration.

    """
    app.logger.info("Request to get Configuration for topic {0}.".format(topicName))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      config_list = []
      config = ConfigResource(ConfigResourceType.TOPIC, topicName)
      topic_configs = admin.describe_configs([config])
      topic_config = topic_configs[0].resources[0]
      for c in topic_config[4] :
        config_list.append({ 'key': c[0], 'value': c[1] })
      return config_list

    except Exception as e:
      ns_topic.abort(500, str(e.args))
    finally:
      admin.close()

  decorators = [requires_Auth]
  @ns_topic.doc(security='basicAuth')
  @ns_topic.expect(config_detail)
  def put(self, topicName):
    """
    Update Topic Configuration.

    """
    ckey = request.json['key']
    cvalue = request.json['value']
    app.logger.info("Request to update configuration for topic {0} for key {1} and value {2}.".format(topicName, ckey, cvalue))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      new_config = ConfigResource(ConfigResourceType.TOPIC, topicName, { ckey: cvalue })
      result = admin.alter_configs([new_config])

    except UnknownTopicOrPartitionError as e:
      api.abort(400, e.description)    
    except Exception as e:
      ns_topic.abort(500, str(e.args))
    finally:
      admin.close()

    if result.resources[0][0] == 0 :
      return { "configured": topicName }
    else :
      api.abort(400, "Bad Request("+result.resources[0][1]+")")    

#####################################################################################################################
# ACL Management
#####################################################################################################################

ns_acl = api.namespace("acl", description="Manage Kafka ACL")

acl_detail = api.model( 'ACLDetail', {'user': fields.String(description="User of ACL Access"), 
                                      'type': fields.String(description="ACL Access Type (READ/WRITE)", enum=['READ', 'WRITE', 'DESCRIBE', 'UNKNOWN'])} ) 

acl_detail_parse = api.parser()
acl_detail_parse.add_argument('user', type=str, help='User of ACL Access')
acl_detail_parse.add_argument('type', type=str, default='READ',
                                choices=('READ', 'WRITE', 'DESCRIBE'),
                                help='ACL Access Type (READ/WRITE)')

@ns_acl.route("/<string:topicName>")
@ns_acl.doc(params={'topicName': 'Topic Name'})
class ACL(Resource):

  decorators = [requires_Auth]
  @ns_acl.doc(security='basicAuth')
  @ns_acl.marshal_with(acl_detail)
  def get(self, topicName):
    """
    Get Topic ACL.

    """
    app.logger.info("Request to get ACL for topic {0}.".format(topicName))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      acl_filter = ACLFilter( principal=None,
                              host="*",
                              operation=ACLOperation.ANY,
                              permission_type=ACLPermissionType.ANY,
                              resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName )
                            )
      acls, error = admin.describe_acls(acl_filter)
      acl_list = []
      for a in acls :
        princpl = a.principal
        oprtn = a.operation.name
        acl_list.append( { 'user': princpl, 'type': oprtn } )
      return acl_list

    except Exception as e:
      ns_acl.abort(500, str(e.args))
    finally:
      admin.close()

  decorators = [requires_Auth]
  @ns_acl.doc(security='basicAuth')
  @ns_acl.expect(acl_detail_parse)
  def put(self, topicName):
    """
    Create Topic ACL.

    """
    args = acl_detail_parse.parse_args()
    auser = args['user']
    atype = args['type']
    acl_user = "User:"+auser
    app.logger.info("Request to create ACL for topic {0} for user {1} and access type {2}.".format(topicName, auser, atype))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      if atype == 'READ' :
        acl1 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.DESCRIBE,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName)
                  )
        acl2 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.READ,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName)
                  )
        acl3 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.READ,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.GROUP, "*")
                  )
        result = admin.create_acls([acl1, acl2, acl3])
        
      elif atype == 'WRITE' :
        acl1 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.DESCRIBE,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName)
                  )
        acl2 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.WRITE,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName)
                  )
        acl3 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.CREATE,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.CLUSTER, "kafka-cluster")
                  )
        result = admin.create_acls([acl1, acl2, acl3])  

      else :
        acl1 = ACL( principal=acl_user,
                    host="*",
                    operation=ACLOperation.DESCRIBE,
                    permission_type=ACLPermissionType.ALLOW,
                    resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName)
                  )
        result = admin.create_acls([acl1])

    except Exception as e:
      ns_acl.abort(500, str(e.args))
    finally:
      admin.close()

    if len(result["failed"]) == 0 :
      return { "creation": "success" }
    else :
      ns_acl.abort(500, "Internal Error(Failure in ACL assignment)")


  decorators = [requires_Auth]
  @ns_acl.doc(security='basicAuth')
  @ns_acl.expect(acl_detail_parse)
  def delete(self, topicName):
    """
    Delete Topic ACL.

    """
    args = acl_detail_parse.parse_args()
    auser = args['user']
    atype = args['type']
    acl_user = "User:"+auser
    app.logger.info("Request to delete ACL for topic {0} for user {1} and access type {2}.".format(topicName, auser, atype))
    try :

      admin = KafkaAdminClient( bootstrap_servers=config['cluster.broker.listeners'], 
                          security_protocol=config['cluster.security.protocol'], 
                          ssl_cafile=config['cluster.ssl.cafile'], 
                          ssl_certfile=config['cluster.ssl.certfile'], 
                          ssl_keyfile=config['cluster.ssl.keyfile'])
      results = admin.delete_acls([ACLFilter( principal=acl_user,
                                              host="*",
                                              operation=ACLOperation.ANY,
                                              permission_type=ACLPermissionType.ANY,
                                              resource_pattern=ResourcePattern(ResourceType.TOPIC, topicName))])

    except Exception as e:
      ns_acl.abort(500, str(e.args))
    finally:
      admin.close()      

    if len(results[0][1]) > 0 :
      return { "delete": "sucess" }
    else :
      ns_acl.abort(500, "Internal Error(Cannot delete any ACL)") 


#####################################################################################################################
# Kafka Cluster Service Management
#####################################################################################################################

ns_service = api.namespace("service", description="Manage Kafka Cluster Services")

service_detail = api.model( 'ServiceDetail', {'host': fields.String(description="Hostname of the Service"), 
                                              'state': fields.String(description="State of the Service", enum=['START', 'STOP'])} ) 

service_detail_parse = api.parser()
service_detail_parse.add_argument('host', type=str, help='Hostnames')
service_detail_parse.add_argument('state', type=str, default='START',
                                choices=('START', 'STOP'),
                                help='Expected Status (START/STOP)')

@ns_service.route("/<string:serviceName>")
@ns_service.doc(params={'serviceName': 'Service Name'})
class Service(Resource):

  decorators = [requires_Auth]
  @ns_service.doc(security='basicAuth')
  @ns_service.marshal_with(service_detail)
  def get(self, serviceName):
    """
    Get Service State.

    """
    app.logger.info("Request to get details for service {0}.".format(serviceName))
    try :

      state_list = []
      if serviceName.lower() == 'kafka' :
        for n in admin.describe_cluster()['brokers'] :
          h = n['host']
          s = remote_execute(h, serviceName.lower(), 'get')
          state_list.append({'host': h, 'state': s})
      return state_list

    except Exception as e:
      ns_service.abort(500, str(e.args))


  decorators = [requires_Auth]
  @ns_service.doc(security='basicAuth')
  @ns_service.expect(service_detail_parse)
  def post(self, serviceName):
    """
    Set Service State.

    """
    args = service_detail_parse.parse_args()
    host = args['host']
    state = args['state']
    app.logger.info("Request for service {0} on host {1} to {2} state.".format(serviceName, host, state))
    try :

      status = remote_execute(host, serviceName.lower(), 'set', state)
      return { "status": status }

    except Exception as e:
      ns_service.abort(500, str(e.args))


#####################################################################################################################
# Application Launcher
#####################################################################################################################

if __name__ == "__main__":
  if 'yes' in config['app.ssl_enabled'] :
    app.run( debug=config['app.debug'], 
             host=config['app.hostname'], 
             port=config['app.port'],
             ssl_context=(config['app.ssl_cert'], config['app.ssl_key']) )
  else :
    app.run( debug=config['app.debug'], 
             host=config['app.hostname'], 
             port=config['app.port'] )
