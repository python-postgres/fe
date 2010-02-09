/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 *//*
 * client3 optimizations.
 */
#define include_client3_functions \
	mFUNC(cat_messages, METH_O, "cat the serialized form of the messages in the given list") \

static PyObject *
cat_messages(PyObject *self, PyObject *messages_in)
{
	const static uint32_t null_attribute = 0xFFFFFFFFL;
	PyObject *msgs = NULL;
	Py_ssize_t nmsgs = 0;
	Py_ssize_t cmsg = 0;

	/*
	 * Buffer holding the messages' serialized form.
	 */
	char *buf = NULL;
	char *nbuf = NULL;
	Py_ssize_t bufsize = 0;
	Py_ssize_t bufpos = 0;

	/*
	 * Get a List object for faster rescanning when dealing with copy data.
	 */
	msgs = PyObject_CallFunctionObjArgs((PyObject *) &PyList_Type, messages_in, NULL);
	if (msgs == NULL)
		return(NULL);
	nmsgs = PyList_GET_SIZE(msgs);

	while (cmsg < nmsgs)
	{
		PyObject *ob;
		ob = PyList_GET_ITEM(msgs, cmsg);

		/*
		 * Choose the path, lots of copy data or more singles to serialize?
		 */
		if (PyBytes_CheckExact(ob))
		{
			Py_ssize_t eofc = cmsg;
			Py_ssize_t xsize = 0;
			/* find the last of the copy data (eofc) */
			do
			{
				++eofc;
				/* increase in size to allocate for the adjacent copy messages */
				xsize += PyBytes_GET_SIZE(ob);
				if (eofc >= nmsgs)
					break; /* end of messages in the list? */

				/* Grab the next message. */
				ob = PyList_GET_ITEM(msgs, eofc);
			} while(PyBytes_CheckExact(ob));

			/*
			 * Either the end of the list or `ob` is not a data object meaning
			 * that it's the end of the copy data.
			 */

			/* realloc the buf for the new copy data */
			xsize = xsize + (5 * (eofc - cmsg));
			bufsize = bufsize + xsize;
			nbuf = realloc(buf, bufsize);
			if (nbuf == NULL)
			{
				PyErr_Format(
					PyExc_MemoryError,
					"failed to allocate %lu bytes of memory for out-going messages",
					(unsigned long) bufsize
				);
				goto fail;
			}
			else
			{
				buf = nbuf;
				nbuf = NULL;
			}

			/*
			 * Make the final pass through the copy lines memcpy'ing the data from
			 * the bytes() objects.
			 */
			while (cmsg < eofc)
			{
				uint32_t msg_length;
				char *localbuf = buf + bufpos + 1;
				buf[bufpos] = 'd'; /* COPY data message type */

				ob = PyList_GET_ITEM(msgs, cmsg);
				msg_length = PyBytes_GET_SIZE(ob) + 4;

				bufpos = bufpos + 1 + msg_length;
				msg_length = local_ntohl(msg_length);
				memcpy(localbuf, &msg_length, 4);
				memcpy(localbuf + 4, PyBytes_AS_STRING(ob), PyBytes_GET_SIZE(ob));
				++cmsg;
			}
		}
		else if (PyTuple_CheckExact(ob))
		{
			/*
			 * Handle 'D' tuple data from a raw Python tuple.
			 */
			Py_ssize_t eofc = cmsg;
			Py_ssize_t xsize = 0;

			/* find the last of the tuple data (eofc) */
			do
			{
				Py_ssize_t current_item, nitems;

				nitems = PyTuple_GET_SIZE(ob);
				if (nitems > 0xFFFF)
				{
					PyErr_SetString(PyExc_OverflowError,
						"too many attributes in tuple message");
					goto fail;
				}

				/*
				 * The items take *at least* 4 bytes each.
				 * (The attribute count is considered later)
				 */
				xsize = xsize + (nitems * 4);

				for (current_item = 0; current_item < nitems; ++current_item)
				{
					PyObject *att = PyTuple_GET_ITEM(ob, current_item);

					/*
					 * Attributes are expected to be bytes() or None.
					 */
					if (PyBytes_CheckExact(att))
						xsize = xsize + PyBytes_GET_SIZE(att);
					else if (att != Py_None)
					{
						PyErr_Format(PyExc_TypeError,
							"cannot serialize tuple message attribute of type '%s'",
							Py_TYPE(att)->tp_name);
						goto fail;
					}
					/*
					 * else it's Py_None and the size has been specified.
					 */
				}

				++eofc;
				if (eofc >= nmsgs)
					break; /* end of messages in the list? */

				/* Grab the next message. */
				ob = PyList_GET_ITEM(msgs, eofc);
			} while(PyTuple_CheckExact(ob));

			/*
			 * Either the end of the list or `ob` is not a data object meaning
			 * that it's the end of the copy data.
			 */

			/*
			 * realloc the buf for the new tuple data
			 *
			 * Each D message consumes at least 1 + 4 + 2 bytes:
			 *  1 for the message type
			 *  4 for the message size
			 *  2 for the attribute count
			 */
			xsize = xsize + (7 * (eofc - cmsg));
			bufsize = bufsize + xsize;
			nbuf = realloc(buf, bufsize);
			if (nbuf == NULL)
			{
				PyErr_Format(
					PyExc_MemoryError,
					"failed to allocate %lu bytes of memory for out-going messages",
					(unsigned long) bufsize
				);
				goto fail;
			}
			else
			{
				buf = nbuf;
				nbuf = NULL;
			}

			/*
			 * Make the final pass through the tuple data memcpy'ing the data from
			 * the bytes() objects.
			 *
			 * No type checks are done here as they should have been done while
			 * gathering the sizes for the realloc().
			 */
			while (cmsg < eofc)
			{
				Py_ssize_t current_item, nitems;
				uint32_t msg_length, out_msg_len;
				uint16_t natts;
				char *localbuf = buf + bufpos + 5; /* skipping the header for now */
				buf[bufpos] = 'D'; /* Tuple data message type */

				ob = PyList_GET_ITEM(msgs, cmsg);
				nitems = PyTuple_GET_SIZE(ob);

				/*
				 * 4 bytes for the message length,
				 * 2 bytes for the attribute count and
				 * 4 bytes for each item in 'ob'.
				 */
				msg_length = 4 + 2 + (nitems * 4);

				/*
				 * Set number of attributes.
				 */
				natts = local_ntohs((uint16_t) nitems);
				Py_MEMCPY(localbuf, &natts, 2);
				localbuf = localbuf + 2;

				for (current_item = 0; current_item < nitems; ++current_item)
				{
					PyObject *att = PyTuple_GET_ITEM(ob, current_item);

					if (att == Py_None)
					{
						Py_MEMCPY(localbuf, &null_attribute, 4);
						localbuf = localbuf + 4;
					}
					else
					{
						Py_ssize_t attsize = PyBytes_GET_SIZE(att);
						uint32_t n_attsize;

						n_attsize = local_ntohl((uint32_t) attsize);

						Py_MEMCPY(localbuf, &n_attsize, 4);
						localbuf = localbuf + 4;
						Py_MEMCPY(localbuf, PyBytes_AS_STRING(att), attsize);
						localbuf = localbuf + attsize;

						msg_length = msg_length + attsize;
					}
				}

				/*
				 * Summed up the message size while copying the attributes.
				 */
				out_msg_len = local_ntohl(msg_length);
				Py_MEMCPY(buf + bufpos + 1, &out_msg_len, 4);

				/*
				 * Filled in the data while summing the message size, so
				 * adjust the buffer position for the next message.
				 */
				bufpos = bufpos + 1 + msg_length;
				++cmsg;
			}
		}
		else
		{
			PyObject *serialized;
			PyObject *msg_type;
			int msg_type_size;
			uint32_t msg_length;

			/*
			 * Call the serialize() method on the element object.
			 * Do this instead of the normal bytes() method to avoid
			 * the type and size packing overhead.
			 */
			serialized = PyObject_CallMethodObjArgs(ob, serialize_strob, NULL);
			if (serialized == NULL)
				goto fail;
			if (!PyBytes_CheckExact(serialized))
			{
				PyErr_Format(
					PyExc_TypeError,
					"%s.serialize() returned object of type %s, expected bytes",
					PyObject_TypeName(ob),
					PyObject_TypeName(serialized)
				);
				goto fail;
			}

			msg_type = PyObject_GetAttr(ob, msgtype_strob);
			if (msg_type == NULL)
			{
				Py_DECREF(serialized);
				goto fail;
			}
			if (!PyBytes_CheckExact(msg_type))
			{
				Py_DECREF(serialized);
				Py_DECREF(msg_type);
				PyErr_Format(
					PyExc_TypeError,
					"message's 'type' attribute was %s, expected bytes",
					PyObject_TypeName(ob)
				);
				goto fail;
			}
			/*
			 * Some elements have empty message types--Startup for instance.
			 * It is important to get the actual size rather than assuming one.
			 */
			msg_type_size = PyBytes_GET_SIZE(msg_type);

			/* realloc the buf for the new copy data */
			bufsize = bufsize + 4 + msg_type_size + PyBytes_GET_SIZE(serialized);
			nbuf = realloc(buf, bufsize);
			if (nbuf == NULL)
			{
				Py_DECREF(serialized);
				Py_DECREF(msg_type);
				PyErr_Format(
					PyExc_MemoryError,
					"failed to allocate %d bytes of memory for out-going messages",
					bufsize
				);
				goto fail;
			}
			else
			{
				buf = nbuf;
				nbuf = NULL;
			}

			/*
			 * All necessary information acquired, so fill in the message's data.
			 */
			buf[bufpos] = *(PyBytes_AS_STRING(msg_type));
			msg_length = PyBytes_GET_SIZE(serialized) + 4;
			msg_length = local_ntohl(msg_length);
			memcpy(buf + bufpos + msg_type_size, &msg_length, 4);
			memcpy(	
				buf + bufpos + 4 + msg_type_size,
				PyBytes_AS_STRING(serialized),
				PyBytes_GET_SIZE(serialized)
			);
			bufpos = bufsize;

			Py_DECREF(serialized);
			Py_DECREF(msg_type);
			++cmsg;
		}
	}

	Py_DECREF(msgs);
	if (buf == NULL)
		/* no messages, no data */
		return(PyBytes_FromString(""));
	else
	{
		PyObject *rob;
		rob = PyBytes_FromStringAndSize(buf, bufsize);
		free(buf);

		return(rob);
	}
fail:
	/* pyerr is expected to be set */
	Py_DECREF(msgs);
	if (buf != NULL)
		free(buf);
	return(NULL);
}
