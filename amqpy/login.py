from __future__ import absolute_import, division, print_function

__metaclass__ = type

import six
import io
from struct import pack

from .serialization import AMQPWriter

__all__ = ['login_responses']

def login_response_amqplain(userid, password):
    response = AMQPWriter()
    response.write_table({'LOGIN': userid, 'PASSWORD': password})
    return response.getvalue()[4:]  # skip the length

def login_response_plain(userid, password):
    """https://tools.ietf.org/html/rfc4616"""
    response = io.BytesIO()
    if isinstance(userid, six.string_types):
        userid = userid.encode('utf-8')
    if isinstance(password, six.string_types):
        password = password.encode('utf-8')
    # authzid
    response.write(userid)
    response.write(pack('B', 0))
    # authcid
    response.write(userid)
    response.write(pack('B', 0))
    # passwd
    response.write(password)
    return response.getvalue()

login_responses = {
    'AMQPLAIN': login_response_amqplain,
    'PLAIN': login_response_plain,
}
