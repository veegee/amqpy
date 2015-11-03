"""Convert between bytestreams and higher-level AMQP types
"""
from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
import six
import io
from datetime import datetime
from decimal import Decimal
from struct import pack, unpack
from time import mktime

from .exceptions import FrameSyntaxError


def byte(n):
    return bytes([n])


class AMQPReader:
    """Read higher-level AMQP types from a bytestream
    """
    __slots__ = ['input', 'bit_count', 'bits']

    def __init__(self, source):
        """
        :param source: source bytes or file-like object
        :type source: io.BytesIO or bytes or bytearray
        """
        if isinstance(source, (bytes, bytearray)):
            self.input = io.BytesIO(source)
        elif isinstance(source, io.BytesIO):
            self.input = source
        else:
            raise TypeError('AMQPReader needs an `io.BytesIO` or `bytes` or `bytearray`')

        self.bit_count = self.bits = 0

    def close(self):
        self.input.close()

    def getvalue(self):
        return self.input.getvalue()

    def read(self, n=-1):
        """Read n bytes
        """
        self.bit_count = self.bits = 0
        return self.input.read(n)

    def read_bit(self):
        """Read a single boolean value
        """
        if not self.bit_count:
            self.bits = ord(self.input.read(1))
            self.bit_count = 8
        result = (self.bits & 1) == 1
        self.bits >>= 1
        self.bit_count -= 1
        return result

    def read_octet(self):
        """Read one byte, return as an integer
        """
        self.bit_count = self.bits = 0
        return unpack('B', self.input.read(1))[0]

    def read_short(self):
        """Read an unsigned 16-bit integer
        """
        self.bit_count = self.bits = 0
        return unpack('>H', self.input.read(2))[0]

    def read_long(self):
        """Read an unsigned 32-bit integer
        """
        self.bit_count = self.bits = 0
        return unpack('>I', self.input.read(4))[0]

    def read_longlong(self):
        """Read an unsigned 64-bit integer
        """
        self.bit_count = self.bits = 0
        return unpack('>Q', self.input.read(8))[0]

    def read_float(self):
        """Read float value."""
        self.bit_count = self.bits = 0
        return unpack('>d', self.input.read(8))[0]

    def read_shortstr(self):
        """Read a short string that's stored in up to 255 bytes

        The encoding isn't specified in the AMQP spec, so assume it's utf-8
        """
        self.bit_count = self.bits = 0
        slen = unpack('B', self.input.read(1))[0]
        return self.input.read(slen).decode('utf-8')

    def read_longstr(self):
        """Read a string that's up to 2**32 bytes

        The encoding isn't specified in the AMQP spec, so assume it's utf-8
        """
        self.bit_count = self.bits = 0
        slen = unpack('>I', self.input.read(4))[0]
        return self.input.read(slen).decode('utf-8')

    def read_table(self):
        """Read an AMQP table, and return as a Python dictionary
        """
        self.bit_count = self.bits = 0
        tlen = unpack('>I', self.input.read(4))[0]
        table_data = AMQPReader(self.input.read(tlen))
        result = {}
        while table_data.input.tell() < tlen:
            name = table_data.read_shortstr()
            val = table_data.read_item()
            result[name] = val
        return result

    def read_item(self):
        ftype = ord(self.input.read(1))

        # 'S': long string
        if ftype == 83:
            val = self.read_longstr()
        # 's': short string
        elif ftype == 115:
            val = self.read_shortstr()
        # 'b': short-short int
        elif ftype == 98:
            val, = unpack('>B', self.input.read(1))
        # 'B': short-short unsigned int
        elif ftype == 66:
            val, = unpack('>b', self.input.read(1))
        # 'U': short int
        elif ftype == 85:
            val, = unpack('>h', self.input.read(2))
        # 'u': short unsigned int
        elif ftype == 117:
            val, = unpack('>H', self.input.read(2))
        # 'I': long int
        elif ftype == 73:
            val, = unpack('>i', self.input.read(4))
        # 'i': long unsigned int
        elif ftype == 105:  # 'l'
            val, = unpack('>I', self.input.read(4))
        # 'L': long long int
        elif ftype == 76:
            val, = unpack('>q', self.input.read(8))
        # 'l': long long unsigned int
        elif ftype == 108:
            val, = unpack('>Q', self.input.read(8))
        # 'f': float
        elif ftype == 102:
            val, = unpack('>f', self.input.read(4))
        # 'd': double
        elif ftype == 100:
            val = self.read_float()
        # 'D': decimal
        elif ftype == 68:
            d = self.read_octet()
            n, = unpack('>i', self.input.read(4))
            val = Decimal(n) / Decimal(10 ** d)
        # 'F': table
        elif ftype == 70:
            val = self.read_table()  # recurse
        # 'A': array
        elif ftype == 65:
            val = self.read_array()
        # 't' (bool)
        elif ftype == 116:
            val = self.read_bit()
        # 'T': timestamp
        elif ftype == 84:
            val = self.read_timestamp()
        # 'V': void
        elif ftype == 86:
            val = None
        else:
            raise FrameSyntaxError('Unknown value in table: {!r} ({!r})'.format(ftype, type(ftype)))
        return val

    def read_array(self):
        array_length = unpack('>I', self.input.read(4))[0]
        array_data = AMQPReader(self.input.read(array_length))
        result = []
        while array_data.input.tell() < array_length:
            val = array_data.read_item()
            result.append(val)
        return result

    def read_timestamp(self):
        """Read and AMQP timestamp, which is a 64-bit integer representing seconds since the Unix
        epoch in 1-second resolution

        Return as a Python datetime.datetime object, expressed as localtime.
        """
        return datetime.fromtimestamp(self.read_longlong())


class AMQPWriter:
    """Convert higher-level AMQP types to bytestreams
    """
    __slots__ = ['out', 'bits', 'bit_count']

    def __init__(self, dest=None):
        """
        Note: dest must also implement `getvalue()`, such as :class:`io.BytesIO`

        :param dest: io.BytesIO object, or None create one
        :type dest: io.IOBase or None
        """
        if isinstance(dest, io.BytesIO):
            self.out = dest
        elif dest is None:
            self.out = io.BytesIO()
        else:
            raise TypeError('AMQPWriter needs an `io.BytesIO` or `None`')

        self.bits = []
        self.bit_count = 0

    def _flush_bits(self):
        if self.bits:
            out = self.out
            for b in self.bits:
                out.write(pack('B', b))
            self.bits = []
            self.bit_count = 0

    def close(self):
        """Pass through if possible to any file-like destinations
        """
        self.out.close()

    def flush(self):
        """Pass through if possible to any file-like destinations
        """
        self.out.flush()

    def getvalue(self):
        """Get what's been encoded so far if we're working with a BytesIO

        :return: bytes
        :rtype: bytes
        """
        self._flush_bits()
        return self.out.getvalue()

    def write(self, b):
        """Write bytes

        :param b: bytes to write
        :type b: bytes
        """
        if six.PY2:
            b = bytes(b)
        self._flush_bits()
        self.out.write(b)

    def write_bit(self, b):
        """Write a boolean value
        """
        b = 1 if b else 0
        shift = self.bit_count % 8
        if shift == 0:
            self.bits.append(0)
        self.bits[-1] |= (b << shift)
        self.bit_count += 1

    def write_octet(self, n):
        """Write an integer as an unsigned 8-bit value
        """
        if n < 0 or n > 255:
            raise FrameSyntaxError(
                'Octet {0!r} out of range 0..255'.format(n))
        self._flush_bits()
        self.out.write(pack('B', n))

    def write_short(self, n):
        """Write an integer as an unsigned 16-bit value
        """
        if n < 0 or n > 65535:
            raise FrameSyntaxError(
                'Octet {0!r} out of range 0..65535'.format(n))
        self._flush_bits()
        self.out.write(pack('>H', int(n)))

    def write_long(self, n):
        """Write an integer as an unsigned2 32-bit value
        """
        if n < 0 or n >= 4294967296:
            raise FrameSyntaxError(
                'Octet {0!r} out of range 0..2**31-1'.format(n))
        self._flush_bits()
        self.out.write(pack('>I', n))

    def write_longlong(self, n):
        """Write an integer as an unsigned 64-bit value
        """
        if n < 0 or n >= 18446744073709551616:
            raise FrameSyntaxError(
                'Octet {0!r} out of range 0..2**64-1'.format(n))
        self._flush_bits()
        self.out.write(pack('>Q', n))

    def write_shortstr(self, s):
        """Write a string up to 255 bytes long (after any encoding)

        If passed a unicode string, encode with UTF-8.
        """
        self._flush_bits()
        if isinstance(s, six.string_types):
            s = s.encode('utf-8')
        if len(s) > 255:
            raise FrameSyntaxError(
                'Shortstring overflow ({0} > 255)'.format(len(s)))
        self.write_octet(len(s))
        self.out.write(s)

    def write_longstr(self, s):
        """Write a string up to 2**32 bytes long after encoding

        If passed a unicode string, encode as UTF-8.
        """
        self._flush_bits()
        if isinstance(s, six.string_types):
            s = s.encode('utf-8')
        self.write_long(len(s))
        self.out.write(s)

    def write_table(self, d):
        """Write out a Python dictionary made of up string keys, and values that are strings,
        signed integers, Decimal, datetime.datetime, or sub-dictionaries following the same
        constraints
        """
        self._flush_bits()
        table_data = AMQPWriter()
        for k, v in d.items():
            table_data.write_shortstr(k)
            table_data.write_item(v, k)
        table_data = table_data.getvalue()
        self.write_long(len(table_data))
        self.out.write(table_data)

    def write_item(self, v, k=None):
        if isinstance(v, (six.string_types, bytes)):
            if six.PY3:
                if isinstance(v, str):
                    v = v.encode('utf-8')
            self.write(b'S')
            self.write_longstr(v)
        elif isinstance(v, bool):
            self.write(pack('>cB', b't', int(v)))
        elif isinstance(v, float):
            self.write(pack('>cd', b'd', v))
        elif isinstance(v, int):
            self.write(pack('>ci', b'I', v))
        elif isinstance(v, Decimal):
            self.write(b'D')
            sign, digits, exponent = v.as_tuple()
            v = 0
            for d in digits:
                v = (v * 10) + d
            if sign:
                v = -v
            self.write_octet(-exponent)
            self.write(pack('>i', v))
        elif isinstance(v, datetime):
            self.write(b'T')
            self.write_timestamp(v)
        elif isinstance(v, dict):
            self.write(b'F')
            self.write_table(v)
        elif isinstance(v, (list, tuple)):
            self.write(b'A')
            self.write_array(v)
        elif v is None:
            self.write(b'V')
        else:
            if k:
                err = 'Table type {!r} for key {!r} not handled by amqpy. [value: {!r}]' \
                    .format(type(v), k, v)
            else:
                err = 'Table type {!r} not handled by amqpy. [value: {!r}]'.format(type(v), v)
            raise FrameSyntaxError(err)

    def write_array(self, a):
        array_data = AMQPWriter()
        for v in a:
            array_data.write_item(v)
        array_data = array_data.getvalue()
        self.write_long(len(array_data))
        self.out.write(array_data)

    def write_timestamp(self, v):
        """Write out a Python datetime.datetime object as a 64-bit integer representing seconds
        since the Unix epoch
        """
        self.out.write(pack('>q', int(mktime(v.timetuple()))))
