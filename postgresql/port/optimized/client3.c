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
			bufsize = bufsize + (5 * (eofc - cmsg)) + xsize;
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
