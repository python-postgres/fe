##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Data module providing a sequence of bytes objects whose value corresponds to its
index in the sequence.

This provides resource for buffer objects to use common message type objects,
and the additional privilege of using the `is` operator in all situations
involving message type comparisons.
"""
message_types = tuple([bytes((x,)) for x in range(256)])
