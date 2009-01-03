##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
PostgreSQL SQLState codes and associated exceptions.

The primary entry point of this module is the `ErrorLookup` function.

For more information see:
 http://www.postgresql.org/docs/current/static/errcodes-appendix.html

This module is executable via -m: python -m postgresql.exceptions.
It provides a convenient way to look up the exception object mapped to by the
given error code::

	$ python -m postgresql.exceptions XX000
	postgresql.exceptions.InternalError [XX000]

If the exact error code is not found, it will try to find the error class's
exception(The first two characters of the error code make up the class
identity)::

	$ python -m postgresql.exceptions XX400
	postgresql.exceptions.InternalError [XX000]
"""
import sys
try:
	from os import linesep
except ImportError:
	linesep = '\n'

class Exception(Exception):
	'Base PostgreSQL exception class'
	pass

class AbortTransaction(Exception):
	"""
	Abort the current transaction and continue and after the edge of the trapped
	exception. In cases where a return value can be emitted, return the
	``returning`` attribute.
	"""
	def __init__(self, *args):
		Exception.__init__(self, *args)
		if args:
			self.returning = args[0]
		else:
			self.returning = None

class UnavailableSSL(Exception):
	"""
	Exception raised when SSL is unavailable by way of it being absent or
	disabled.

	This is an exception provided for PostgreSQL clients; no code is mapped to it
	because it's a client only exception.
	"""

def sixbit(i):
	'force values to be in the sixbit range'
	return ((i - 0x30) & 0x3F)
def unsixbit(i):
	'extract the original value from an integer processed with `sixbit`'
	return ((i & 0x3F) + 0x30)

def make(chars):
	"""
	Given an SQL state code as a character string, create the integer
	representation using `sixbit`.
	"""
	return sixbit(ord(chars[0])) + (sixbit(ord(chars[1])) << 6) + \
			(sixbit(ord(chars[2])) << 12) + (sixbit(ord(chars[3])) << 18) + \
			(sixbit(ord(chars[4])) << 24)

def unmake(code):
	"""
	Given an SQL state code as an integer, create the character string
	representation using `unsixbit`.
	"""
	return chr(unsixbit(code)) + chr(unsixbit(code >> 6)) + \
		chr(unsixbit(code >> 12)) + chr(unsixbit(code >> 18)) + \
		chr(unsixbit(code >> 24))

class State(int):
	"""
	An SQL state code. Normally used to identify the kind of error that occurred.
	"""
	def __new__(self, arg):
		if isinstance(arg, State):
			return arg
		elif isinstance(arg, int):
			chars = unmake(arg)
		else:
			chars = ''.join(arg)
			arg = make(chars)

		rob = int.__new__(self, arg)
		rob._chars = chars
		return rob

	def __str__(self):
		return self._chars

	def __repr__(self):
		return '%s.%s(%r)' %(
			type(self).__module__,
			type(self).__name__,
			self._chars,
		)

	def __getitem__(self, item):
		return self._chars[item]

class Class(State):
	"""
	SQL state code class. This is a state code whose last three characters are
	'000'. Classes are special in the sense that they 
	"""
	def __new__(self, chars, **kw):
		if isinstance(chars, Class):
			return chars

		rob = State.__new__(self, (chars[0], chars[1], '0', '0', '0'))
		for k, v in kw.items():
			setattr(rob, k, v)
		return rob

	def __getitem__(self, val):
		val = State(val)
		return [x for x in self.__dict__ if self.__dict__[x] == val][0]

	def __repr__(self):
		ordered_states = [
			(k, v._chars[2:]) for k, v in self.__dict__.items()
 			if isinstance(v, State)
		]
		ordered_states.sort(key = lambda x: x[1])
		states = ',\n\t'.join([
			'%s = %r' %(k, v)
			for k, v in ordered_states
		])
		return '%s.%s(%r%s%s%s)' %(
			type(self).__module__,
			type(self).__name__,
			self._chars[0:2],
			states and ',\n\t',
			states,
			states and '\n',
		)

	def __contains__(self, ob):
		"""
		Whether the given state, `ob`, is in the state-class, `self`.

		>>> State('Ex000') in Class('Ex')
		True
		>>> State('EX000') in Class('Ex')
		False
		"""
		return State(ob)._chars[0:2] == self._chars[0:2]

	def __setattr__(self, att, val):
		"""
		Create a state code in the class. If the attribute name starts with '_', a
		normal set will occur.

		>>> cls = Class('Ex')
		>>> cls.ERROR = '001'
		>>> cls.ERROR
		postgresql.exceptions.State('Ex001')
		"""
		if att.startswith('_'):
			super(Class, self).__setattr__(att, val)
		else:
			if isinstance(val, int):
				c = State(val)
			else:
				c = State((self._chars[0], self._chars[1], val[0], val[1], val[2]))
			super(Class, self).__setattr__(att, c)

	def __iter__(self):
		return iter([
			x for x in self.__dict__.values() if isinstance(x, State)
		])

class Mapping(dict):
	'Dictionary subclass for mapping states and classes to Python classes'
	__slots__ = ()
	def get(self, key):
		'Gets the value that at the key or the Class of that key or None'
		c = State(key)
		supr = super(Mapping, self)
		return supr.get(c) or supr.get(Class(c))

	def set(self, code, cls):
		'Assumes that it will be given a class for the value argument'
		code = State(code)
		codec = Class(code)
		if codec != SUCCESS:
			cur = self.get(code)
			if cur is None or codec != code or \
				(issubclass(cur, cls) and codec == code):
				self[code] = cls

SUCCESS = Class('00',
	COMPLETION = '000',
)

WARNING = Class('01',
	DYNAMIC_RESULT_SETS_RETURNED = '00C',
	IMPLICIT_ZERO_BIT_PADDING = '008',
	NULL_VALUE_ELIMINATED_IN_SET_FUNCTION = '003',
	PRIVILEGE_NOT_GRANTED = '007',
	PRIVILEGE_NOT_REVOKED = '006',
	STRING_DATA_RIGHT_TRUNCATION = '004',
	DEPRECATED_FEATURE = 'P01',
)

NO_DATA_WARNING = Class('02',
	NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED = '001',
)

SQL_INCOMPLETE_STATEMENT = Class('03')

CONNECTION = Class('08',
	DOES_NOT_EXIST = '003',
	FAILURE = '006',
	SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION = '001',
	SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION = '004',
	TRANSACTION_RESOLUTION_UNKNOWN = '007',
	PROTOCOL_VIOLATION = 'P01',
)

TRIGGERED_ACTION = Class('09')
FEATURE_NOT_SUPPORTED = Class('0A')

LOCATOR = Class('0F')
LOCATOR.INVALID_SPECIFICATION = '001'

GRANTOR = Class('0L',
	OPERATION = 'P01',
)

ROLE_SPECIFICATION = Class('0P')

CARDINALITY_VIOLATION = Class('21')

DATA = Class('22',
	ARRAY_ELEMENT = '02E',
	ASSIGNMENT = '005',
	CHARACTER_NOT_IN_REPERTOIRE = '021',
	DATETIME_FIELD_OVERFLOW = '008',
	DIVISION_BY_ZERO = '012',
	ESCAPE_CHARACTER_CONFLICT = '00B',
	INDICATOR_OVERFLOW = '022',
	INTERVAL_FIELD_OVERFLOW = '015',
	INVALID_ARGUMENT_FOR_LOG = '01E',
	INVALID_ARGUMENT_FOR_POWER_FUNCTION = '01F',
	INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION = '01G',
	INVALID_CHARACTER_VALUE_FOR_CAST = '018',
	INVALID_ESCAPE_CHARACTER = '019',
	INVALID_DATETIME_FORMAT = '007',
	INVALID_ESCAPE_OCTET = '00D',
	INVALID_ESCAPE_SEQUENCE = '025',
	INVALID_INDICATOR_PARAMETER_VALUE = '010',
	INVALID_LIMIT_VALUE = '020',
	INVALID_PARAMETER_VALUE = '023',
	INVALID_REGULAR_EXPRESSION = '01B',
	INVALID_TIME_ZONE_DISPLACEMENT_VALUE = '009',
	INVALID_USE_OF_ESCAPE_CHARACTER = '00C',
	MOST_SPECIFIC_TYPE_MISMATCH = '00G',
	NULL_VALUE_NOT_ALLOWED = '004',
	NULL_VALUE_NO_INDICATOR_PARAMETER = '002',
	NUMERIC_VALUE_OUT_OF_RANGE = '003',
	STRING_LENGTH_MISMATCH = '026',
	STRING_RIGHT_TRUNCATION = '001',
	SUBSTRING = '011',
	TRIM_ERROR = '027',
	UNTERMINATED_C_STRING = '024',
	ZERO_LENGTH_CHARACTER_STRING = '00F',

	NOT_AN_XML_DOCUMENT = '00L',
	INVALID_XML_DOCUMENT = '00M',
	INVALID_XML_CONTENT = 'OON',
	INVALID_XML_COMMENT = 'OOS',
	INVALID_XML_PROCESSING_INSTRUCTION = 'OOT',

	FLOATING_POINT = 'P01',
	INVALID_TEXT_REPRESENTATION = 'P02',
	INVALID_BINARY_REPRESENTATION = 'P03',
	BAD_COPY_FILE_FORMAT = 'P04',
	UNTRANSLATABLE_CHARACTER = 'P05',
	NONSTANDARD_USE_OF_ESCAPE_CHARACTER = 'P06',
)

# Integrity Constraint Violation
ICV = Class('23',
	RESTRICT = '001',
	NOT_NULL = '502',
	FOREIGN_KEY = '503',
	UNIQUE = '505',
	CHECK = '514',
)

# Invalid Transaction State
ITS = Class('25',
	ACTIVE = '001',
	BRANCH_ALREADY_ACTIVE = '002',
	HELD_CURSOR_REQUIRES_SAME_ISOLATION = '008',
	INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH = '003',
	INAPPROPRIATE_ISOLATION_FOR_BRANCH = '004',
	NO_ACTIVE_FOR_BRANCH = '005',
	READ_ONLY = '006',
	SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED = '007',
	NO_ACTIVE = 'P01',
	IN_FAILED = 'P02',
)

TRIGGERED_DATA_CHANGE_VIOLATION = Class('27')
AUTHORIZATION_SPECIFICATION = Class('28')

# Dependent Privilege Descriptors Still Exist
DPDSE = Class('2B',
	OBJECTS = 'P01',
)

INVALID_CATALOG_NAME = Class('3D')
INVALID_CURSOR_NAME = Class('34')
INVALID_CURSOR_STATE = Class('24')
INVALID_SCHEMA_NAME = Class('3F')
INVALID_STATEMENT_NAME = Class('26')
INVALID_TRANSACTION_INITIATION = Class('0B')
INVALID_TRANSACTION_TERMINATION = Class('2D')

# SQL Routine Exception
SRE = Class('2F',
	FUNCTION_EXECUTED_NO_RETURN_STATEMENT = '005',
	MODIFYING_DATA_NOT_PERMITTED = '002',
	PROHIBITED_STATEMENT_ATTEMPTED = '003',
	READING_DATA_NOT_PERMITTED = '004',
)

# External Routine Exception
ERE = Class('38',
	CONTAINING_SQL_NOT_PERMITTED = '001',
	MODIFYING_SQL_DATA_NOT_PERMITTED = '002',
	PROHIBITED_SQL_STATEMENT_ATTEMPTED = '003',
	READING_SQL_DATA_NOT_PERMITTED = '004',
)

# External Routine Invocation Exception
ERIE = Class('39',
	INVALID_SQLSTATE_RETURNED = '001',
	NULL_VALUE_NOT_ALLOWED = '004',
	TRIGGER_PROTOCOL_VIOLATED = 'P01',
	SRF_PROTOCOL_VIOLATED = 'P02',
)

SAVEPOINT = Class('3B',
	INVALID_SPECIFICATION = '001',
)

# Transaction Rollback
TR = Class('40',
	INTEGRITY_CONSTRAINT_VIOLATION = '002',
	SERIALIZATION_FAILURE = '001',
	STATEMENT_COMPLETION_UNKNOWN = '003',
	DEADLOCK_DETECTED = 'P01',
)

# Syntax Error or Access Rule Violation
SEARV = Class('42',
	SYNTAX = '601',
	INSUFFICIENT_PRIVILEGE = '501',
	CANNOT_COERCE = '846',
	GROUPING = '803',
	INVALID_FOREIGN_KEY = '830',
	INVALID_NAME = '602',
	NAME_TOO_LONG = '622',
	RESERVED_NAME = '939',
	DATATYPE_MISMATCH = '804',
	INDETERMINATE_DATATYPE = 'P18',
	WRONG_OBJECT_TYPE = '809',
	UNDEFINED_COLUMN = '703',
	UNDEFINED_FUNCTION = '883',
	UNDEFINED_TABLE = 'P01',
	UNDEFINED_PARAMETER = 'P02',
	UNDEFINED_OBJECT = '704',
	DUPLICATE_COLUMN = '701',
	DUPLICATE_CURSOR = 'P03',
	DUPLICATE_DATABASE = 'P04',
	DUPLICATE_FUNCTION = '723',
	DUPLICATE_PSTATEMENT = 'P05',
	DUPLICATE_SCHEMA = 'P06',
	DUPLICATE_TABLE = 'P07',
	DUPLICATE_ALIAS = '712',
	DUPLICATE_OBJECT = '710',
	AMBIGUOUS_COLUMN = '702',
	AMBIGUOUS_FUNCTION = '725',
	AMBIGUOUS_PARAMETER = 'P08',
	AMBIGUOUS_ALIAS = 'P09',
	INVALID_COLUMN_REFERENCE = 'P10',
	INVALID_COLUMN_DEFINITION = '611',
	INVALID_CURSOR_DEFINITION = 'P11',
	INVALID_DATABASE_DEFINITION = 'P12',
	INVALID_FUNCTION_DEFINITION = 'P13',
	INVALID_PSTATEMENT_DEFINITION = 'P14',
	INVALID_SCHEMA_DEFINITION = 'P15',
	INVALID_TABLE_DEFINITION = 'P16',
	INVALID_OBJECT_DEFINITION = 'P17',
)

WITH_CHECK_OPTION_VIOLATION = Class('44')

# Insufficient Resources
IR = Class('53',
	DISK_FULL = '100',
	OUT_OF_MEMORY = '200',
	CONNECTION_OVERFLOW = '300'
)

# Program Limit Exceeded
PLE = Class('54',
	COMPLEXITY = '001',
	COLUMN = '011',
	ARGUMENT = '023'
)

# Object Not In Prerequisite State
ONIPS = Class('55',
	OBJECT_IN_USE = '006',
	CANT_CHANGE_RUNTIME_PARAM = 'P02',
	LOCK_NOT_AVAILABLE = 'P03'
)

# Operator Intervention
OI = Class('57',
	QUERY_CANCELED = '014',
	ADMIN_SHUTDOWN = 'P01',
	CRASH_SHUTDOWN = 'P02',
	CANNOT_CONNECT_NOW = 'P03',
)

# System IO Error
SIO = Class('58',
	UNDEFINED_FILE = 'P01',
	DUPLICATE_FILE = 'P02',
)

# Configuration File Error
CFE = Class('F0',
	LOCK_FILE_EXISTS = '001',
)

PLPGSQL = Class('P0',
	RAISE = '001',
	NO_DATA_FOUND = '002',
	TOO_MANY_ROWS = '003',
)

# Internal Error
IE = Class('XX',
	DATA_CORRUPTED = '001',
	INDEX_CORRUPTED = '002',
)

def msgstr(ob):
	'Create a string for display in a warning or traceback'
	message = ob.message
	details = ob.details
	loc = [
		details.get('file'),
		details.get('line'),
		details.get('function')
	]
	# If there are any location details, make the locstr.
	if loc.count(None) < 3:
		locstr = '%sLOCATION: File %r, line %s, in %s' %(
			linesep,
			loc[0] or '?',
			loc[1] or '?',
			loc[2] or '?',
		)
	else:
		locstr = ''

	return str(message or details.get('message')) + (details is not None and
		linesep + linesep.join(
			[
				'%s: %s' %(k.upper(), v) for k, v in details.items()
				if k not in ('message', 'severity', 'file', 'function', 'line')
			]
		) or '') + locstr

class Warning(Warning):
	code = WARNING
	message = None
	__str__ = msgstr

	def __init__(self, msg, code = None, details = {}):
		self.message = msg
		if code is not None and self.code != code:
			self.code = code
		self.details = details

class DeprecationWarning(Warning, DeprecationWarning):
	code = WARNING.DEPRECATED_FEATURE
class DynamicResultSetsReturnedWarning(Warning):
	code = WARNING.DYNAMIC_RESULT_SETS_RETURNED
class ImplicitZeroBitPaddingWarning(Warning):
	code = WARNING.IMPLICIT_ZERO_BIT_PADDING
class NullValueEliminatedInSetFunctionWarning(Warning):
	code = WARNING.NULL_VALUE_ELIMINATED_IN_SET_FUNCTION
class PrivilegeNotGrantedWarning(Warning):
	code = WARNING.PRIVILEGE_NOT_GRANTED
class PrivilegeNotRevokedWarning(Warning):
	code = WARNING.PRIVILEGE_NOT_REVOKED
class StringDataRightTruncationWarning(Warning):
	code = WARNING.STRING_DATA_RIGHT_TRUNCATION

class NoDataWarning(Warning):
	code = NO_DATA_WARNING

class NoMoreSetsReturned(NoDataWarning):
	code = NO_DATA_WARNING.NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED 

class Error(Exception):
	"""Error(msg[, code])
	
	Implements an interface to a PostgreSQL-style exception. Use expectation is
	for producing Postgres ERRORs from a raised Error and raising an Error from a
	given PostgreSQL ERROR from a connection, result, or trapped backend ERROR.
	"""
	code = IE
	details = None
	display_order = ('detail', 'hint', 'context')

	# We explicitly set message as the error's message, so avoid 2.6's deprecation
	# warning as it does not apply to pg_exc.Error.
	message = None

	def __init__(self, msg, code = None, details = None):
		if code is not None and self.code != code:
			self.code = code
		if details is not None:
			self.details = details
		self.message = msg

	__str__ = msgstr
	def __repr__(self):
		return '%s.%s(%r%s%r)' %(
			type(self).__module__,
			type(self).__name__,
			self.message,
			self.details and ', ',
			self.details
		)

# Abstract Exceptions
class OverflowError(Error):
	pass
class TransactionError(Error):
	pass
class IntegrityError(Error):
	pass
class ValidityError(Error):
	pass

class FeatureError(Error):
	code = FEATURE_NOT_SUPPORTED

class ConnectionError(Error):
	code = CONNECTION
class ConnectionDoesNotExistError(ConnectionError):
	code = CONNECTION.DOES_NOT_EXIST
class ConnectionFailureError(ConnectionError):
	code = CONNECTION.FAILURE
class ProtocolError(ConnectionError):
	code = CONNECTION.PROTOCOL_VIOLATION
class SQLClientUnableToEstablishSQLConnectionError(ConnectionError):
	code = CONNECTION.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
class SQLServerRejectedEstablishmentOfSQLConnection(ConnectionError):
	code = CONNECTION.SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
class TransactionResolutionUnknownError(ConnectionError):
	code = CONNECTION.TRANSACTION_RESOLUTION_UNKNOWN

class TriggeredActionError(Error):
	code = TRIGGERED_ACTION 

class AuthenticationSpecificationError(Error):
	code = AUTHORIZATION_SPECIFICATION 

class DPDSEError(Error):
	"Dependent Privilege Descriptors Still Exist"
	code = DPDSE
class DPDSEObjectError(DPDSEError): code = DPDSE.OBJECTS

class SREError(Error):
	"SQL Routine Exception"
	code = SRE
class FunctionExecutedNoReturnStatementError(SREError):
	code = SRE.FUNCTION_EXECUTED_NO_RETURN_STATEMENT
class DataModificationProhibitedError(SREError):
	code = SRE.MODIFYING_DATA_NOT_PERMITTED
class ProhibitedStatementError(SREError):
	code = SRE.PROHIBITED_STATEMENT_ATTEMPTED
class ReadingDataProhibitedError(SREError):
	code = SRE.READING_DATA_NOT_PERMITTED

class EREError(Error):
	"External Routine Exception"
	code = ERE
class SQLContainmentProhibitedError(EREError):
	code = ERE.CONTAINING_SQL_NOT_PERMITTED
class SQLDataModificationProhibitedError(EREError):
	code = ERE.MODIFYING_SQL_DATA_NOT_PERMITTED
class SQLProhibitedStatementAttemptError(EREError):
	code = ERE.PROHIBITED_SQL_STATEMENT_ATTEMPTED
class SQLDataReadProhibitedError(EREError):
	code = ERE.READING_SQL_DATA_NOT_PERMITTED

class ERIEError(Error):
	"External Routine Invocation Exception"
	code = ERIE
class InvalidSQLState(ERIEError):
	code = ERIE.INVALID_SQLSTATE_RETURNED
class NullValueNotAllowed(ERIEError):
	code = ERIE.NULL_VALUE_NOT_ALLOWED
class TriggerProtocolError(ERIEError):
	code = ERIE.TRIGGER_PROTOCOL_VIOLATED
class SRFProtocolError(ERIEError):
	code = ERIE.SRF_PROTOCOL_VIOLATED

class TRError(TransactionError):
	"Transaction Rollback"
	code = TR
class DeadlockError(TRError): code = TR.DEADLOCK_DETECTED
class IntegrityConstraintViolationError(TRError):
	code = TR.INTEGRITY_CONSTRAINT_VIOLATION
class SerializationError(TRError):
	code = TR.SERIALIZATION_FAILURE
class StatementCompletionUnknownError(TRError):
	code = TR.STATEMENT_COMPLETION_UNKNOWN

class ITSError(TransactionError):
	"Invalid Transaction State"
	code = ITS
class ReadOnlyTransactionError(ITSError):
	"Occurs when an alteration occurs in a read-only transaction."
	code = ITS.READ_ONLY
class ActiveTransactionError(ITSError):
	code = ITS.ACTIVE
class NoActiveTransactionError(ITSError):
	code = ITS.NO_ACTIVE
class InFailedTransactionError(ITSError):
	"Occurs when an action occurs in a failed transaction."
	code = ITS.IN_FAILED

class SavepointError(TransactionError):
	"Classification error designating errors that relate to savepoints."
	code = SAVEPOINT
class InvalidSavepointSpecificationError(SavepointError):
	code = SAVEPOINT.INVALID_SPECIFICATION

class InvalidTransactionInitiation(TransactionError):
	code = INVALID_TRANSACTION_INITIATION
class InvalidTransactionTermination(TransactionError):
	code = INVALID_TRANSACTION_TERMINATION

class IRError(OverflowError):
	"Insufficient Resource Errors"
	code = IR
class MemoryError(IRError, MemoryError): code = IR.OUT_OF_MEMORY
class DiskFullError(IRError): code = IR.DISK_FULL
class ConnectionOverflowError(IRError, ConnectionError):
	code = IR.CONNECTION_OVERFLOW

class LocatorError(Error): code = LOCATOR
class InvalidLocatorSpecificationError(Error):
	code = LOCATOR.INVALID_SPECIFICATION

class CardinalityError(Error):
	code = CARDINALITY_VIOLATION

class GrantorError(Error): code = GRANTOR
class GrantorOperationError(GrantorError): code = GRANTOR.OPERATION

class RoleSpecificationError(Error):
	code = ROLE_SPECIFICATION

class PLEError(OverflowError):
	"Program Limit Exceeded"
	code = PLE
class ComplexityOverflowError(PLEError):
	code = PLE.COMPLEXITY
class ColumnOverflowError(PLEError):
	code = PLE.COLUMN
class ArgumentOverflowError(PLEError):
	code = PLE.ARGUMENT

class ONIPSError(Error):
	"Object Not In Prerequisite State"
	code = ONIPS
class ObjectInUseError(ONIPSError):
	code = ONIPS.OBJECT_IN_USE
class ImmutableRuntimeParameterError(ONIPSError):
	code = ONIPS.CANT_CHANGE_RUNTIME_PARAM
class UnavailableLockError(ONIPSError):
	code = ONIPS.LOCK_NOT_AVAILABLE

class SEARVError(Error):
	"Syntax Error or Access Rule Violation"
	code = SEARV
class SyntaxError(SEARVError):
	code = SEARV.SYNTAX

class TypeError(SEARVError):
	pass
class CoercionError(TypeError):
	code = SEARV.CANNOT_COERCE
class DatatypeMismatchError(TypeError):
	code = SEARV.DATATYPE_MISMATCH
class IndeterminateDatatypeError(TypeError):
	code = SEARV.INDETERMINATE_DATATYPE

class UndefinedError(SEARVError):
	pass
class UndefinedColumnError(UndefinedError):
	code = SEARV.UNDEFINED_COLUMN
class UndefinedFunctionError(UndefinedError):
	code = SEARV.UNDEFINED_FUNCTION
class UndefinedTableError(UndefinedError):
	code = SEARV.UNDEFINED_TABLE
class UndefinedParameterError(UndefinedError):
	code = SEARV.UNDEFINED_PARAMETER
class UndefinedObjectError(UndefinedError):
	code = SEARV.UNDEFINED_OBJECT

class DuplicateError(SEARVError):
	pass
class DuplicateColumnError(DuplicateError):
	code = SEARV.DUPLICATE_COLUMN
class DuplicateCursorError(DuplicateError):
	code = SEARV.DUPLICATE_CURSOR
class DuplicateDatabaseError(DuplicateError):
	code = SEARV.DUPLICATE_DATABASE
class DuplicateFunctionError(DuplicateError):
	code = SEARV.DUPLICATE_FUNCTION
class DuplicatePreparedStatementError(DuplicateError):
	code = SEARV.DUPLICATE_PSTATEMENT
class DuplicateSchemaError(DuplicateError):
	code = SEARV.DUPLICATE_SCHEMA
class DuplicateTableError(DuplicateError):
	code = SEARV.DUPLICATE_TABLE
class DuplicateAliasError(DuplicateError):
	code = SEARV.DUPLICATE_ALIAS
class DuplicateObjectError(DuplicateError):
	code = SEARV.DUPLICATE_OBJECT

class AmbiguityError(SEARVError):
	pass
class AmbiguousColumnError(AmbiguityError):
	code = SEARV.AMBIGUOUS_COLUMN
class AmbiguousFunctionError(AmbiguityError):
	code = SEARV.AMBIGUOUS_FUNCTION
class AmbiguousParameterError(AmbiguityError):
	code = SEARV.AMBIGUOUS_PARAMETER
class AmbiguousAliasError(AmbiguityError):
	code = SEARV.AMBIGUOUS_ALIAS

class SEARVValidityError(SEARVError, ValidityError):
	pass
class InvalidColumnReference(SEARVValidityError):
	code = SEARV.INVALID_COLUMN_REFERENCE
class InvalidColumnDefinition(SEARVValidityError):
	code = SEARV.INVALID_COLUMN_DEFINITION
class InvalidCursorDefinition(SEARVValidityError):
	code = SEARV.INVALID_CURSOR_DEFINITION
class InvalidDatabaseDefinition(SEARVValidityError):
	code = SEARV.INVALID_DATABASE_DEFINITION
class InvalidFunctionDefinition(SEARVValidityError):
	code = SEARV.INVALID_FUNCTION_DEFINITION
class InvalidPreparedStatementDefinition(SEARVValidityError):
	code = SEARV.INVALID_PSTATEMENT_DEFINITION
class InvalidSchemaDefinition(SEARVValidityError):
	code = SEARV.INVALID_SCHEMA_DEFINITION
class InvalidTableDefinition(SEARVValidityError):
	code = SEARV.INVALID_TABLE_DEFINITION
class InvalidObjectDefinition(SEARVValidityError):
	code = SEARV.INVALID_OBJECT_DEFINITION

class InvalidCursorState(ValidityError):
	code = INVALID_CURSOR_STATE

class WithCheckOptionError(Error):
	code = WITH_CHECK_OPTION_VIOLATION 

class InvalidCatalogName(Error, NameError):
	code = INVALID_CATALOG_NAME
class InvalidCursorName(Error, NameError):
	code = INVALID_CURSOR_NAME
class InvalidStatementName(Error, NameError):
	code = INVALID_STATEMENT_NAME
class InvalidSchemaName(Error, NameError):
	code = INVALID_SCHEMA_NAME

class ICVError(IntegrityError):
	"Integrity Contraint Violation"
	code = ICV
class RestrictError(ICVError):
	code = ICV.RESTRICT
class NotNullError(ICVError):
	code = ICV.NOT_NULL
class ForeignKeyError(ICVError):
	code = ICV.FOREIGN_KEY
class UniqueError(ICVError):
	code = ICV.UNIQUE
class CheckError(ICVError):
	code = ICV.CHECK


class DataError(Error):
	code = DATA

class NullError(DataError):
	pass
class NullValueNotAllowedError(NullError):
	code = DATA.NULL_VALUE_NOT_ALLOWED
class NullValueNoIndicatorParameter(NullError):
	code = DATA.NULL_VALUE_NO_INDICATOR_PARAMETER

class ZeroDivisionError(DataError, ZeroDivisionError):
	code = DATA.DIVISION_BY_ZERO
class FloatingPointError(DataError, FloatingPointError):
	code = DATA.FLOATING_POINT
class DateTimeFieldOverflowError(DataError):
	code = DATA.DATETIME_FIELD_OVERFLOW
class AssignmentError(DataError):
	code = DATA.ASSIGNMENT
class BadCopyError(DataError):
	code = DATA.BAD_COPY_FILE_FORMAT

class InvalidTextRepresentationError(DataError):
	code = DATA.INVALID_TEXT_REPRESENTATION
class InvalidBinaryRepresentationError(DataError):
	code = DATA.INVALID_BINARY_REPRESENTATION
class UntranslatableCharacterError(DataError):
	code = DATA.UNTRANSLATABLE_CHARACTER
class NonstandardUseOfEscapeCharacterError(DataError):
	code = DATA.NONSTANDARD_USE_OF_ESCAPE_CHARACTER 

class DataValidityError(DataError, ValidityError):
	"DataError regarding validity"

class NotAnXmlDocumentError(DataValidityError):
	code = DATA.NOT_AN_XML_DOCUMENT
class InvalidXmlDocument(DataValidityError):
	code = DATA.INVALID_XML_DOCUMENT
class InvalidXmlContent(DataValidityError):
	code = DATA.INVALID_XML_CONTENT
class InvalidXmlComment(DataValidityError):
	code = DATA.INVALID_XML_COMMENT
class InvalidXmlProcessingInstruction(DataValidityError):
	code = DATA.INVALID_XML_PROCESSING_INSTRUCTION

class InvalidArgumentForLogError(DataValidityError):
	code = DATA.INVALID_ARGUMENT_FOR_LOG
class InvalidArgumentForPowerFunctionError(DataValidityError):
	code = DATA.INVALID_ARGUMENT_FOR_POWER_FUNCTION
class InvalidArgumentForWidthBucketFunctionError(DataValidityError):
	code = DATA.INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION
class InvalidCharacterValueForCastError(DataValidityError):
	code = DATA.INVALID_CHARACTER_VALUE_FOR_CAST
class InvalidDateTimeFormatError(DataValidityError):
	code = DATA.INVALID_DATETIME_FORMAT
class InvalidEscapeCharacterError(DataValidityError):
	code = DATA.INVALID_ESCAPE_CHARACTER
class InvalidEscapeOctet(DataValidityError):
	code = DATA.INVALID_ESCAPE_OCTET
class InvalidEscapeSequenceError(DataValidityError):
	code = DATA.INVALID_ESCAPE_SEQUENCE
class InvalidIndicatorParameterValue(DataValidityError):
	code = DATA.INVALID_INDICATOR_PARAMETER_VALUE
class InvalidLimitValueError(DataValidityError):
	code = DATA.INVALID_LIMIT_VALUE
class InvalidParameterValue(DataValidityError):
	code = DATA.INVALID_PARAMETER_VALUE
class InvalidRegularExpressionError(DataValidityError):
	code = DATA.INVALID_REGULAR_EXPRESSION
class InvalidTimeZoneDisplacementValueError(DataValidityError):
	code = DATA.INVALID_TIME_ZONE_DISPLACEMENT_VALUE
class InvalidUseOfEscapeCharacterError(DataValidityError):
	code = DATA.INVALID_USE_OF_ESCAPE_CHARACTER

class InternalError(Error):
	code = IE
class DataCorruptedError(InternalError):
	code = IE.DATA_CORRUPTED
class IndexCorruptedError(InternalError):
	code = IE.INDEX_CORRUPTED

class SIOError(Error):
	"System I/O"
	code = SIO
class UndefinedFileError(SIOError):
	code = SIO.UNDEFINED_FILE
class DuplicateFileError(SIOError):
	code = SIO.DUPLICATE_FILE

class CFError(Error):
	"Configuration File Error"
	code = CFE
class LockFileExistsError(CFError):
	code = CFE.LOCK_FILE_EXISTS

class OIError(Error):
	"Operator Intervention"
	code = OI
class QueryCanceledError(OIError):
	code = OI.QUERY_CANCELED
class AdminShutdownError(OIError):
	code = OI.ADMIN_SHUTDOWN
class CrashShutdownError(OIError):
	code = OI.CRASH_SHUTDOWN
class CannotConnectNowError(OIError):
	code = OI.CANNOT_CONNECT_NOW

class PLPGSQLError(Error):
	"Error raised by a PL/PgSQL procedural function"
	code = PLPGSQL
class PLPGSQLRaiseError(PLPGSQLError):
	"Error raised by a PL/PgSQL RAISE statement."
	code = PLPGSQL.RAISE
class PLPGSQLNoDataFoundError(PLPGSQLError):
	code = PLPGSQL.NO_DATA_FOUND
class PLPGSQLTooManyRowsError(PLPGSQLError):
	code = PLPGSQL.TOO_MANY_ROWS

CodeClass = Mapping()
def ErrorLookup(c):
	"""
	Given an error code, return the exception that is most closely associated
	with it.
	"""
	return CodeClass.get(c) or Error

# Setup mapping to provide code based exception lookup.
# Ideally, this would be placed in a metaclass, but oh well...
d = sys.modules[__name__].__dict__
e = None
for e in d.values():
	if type(e) == type(Error) and issubclass(e, (Error,Warning)) and e not in (
		Error, TransactionError, IntegrityError, ValidityError, OverflowError
	) and hasattr(e, 'code') and Class(e.code) != SUCCESS:
		CodeClass.set(e.code, e)
del e, d

if __name__ == '__main__':
	for x in sys.argv[1:]:
		e = ErrorLookup(x)
		sys.stdout.write('postgresql.exceptions.%s [%s]%s%s' %(
				e.__name__, e.code, linesep, (
					e.__doc__ is not None and linesep.join([
						'  ' + x for x in (e.__doc__).split('\n')
					]) + linesep or ''
				)
			)
		)
##
# vim: ts=3:sw=3:noet:
