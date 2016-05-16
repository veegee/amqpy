from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type

from .serialization import AMQPWriter

__all__ = ['login_responses']

def login_response_amqplain(userid, password):
    response = AMQPWriter()
    response.write_table({'LOGIN': userid, 'PASSWORD': password})
    return response.getvalue()[4:]  # skip the length

login_responses = {
    'AMQPLAIN': login_response_amqplain,
}
