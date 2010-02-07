/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 *//*
 * element3 optimizations.
 */
#define include_element3_functions \
	mFUNC(parse_tuple_message, METH_VARARGS, "parse the given tuple data into a tuple of raw data") \
	mFUNC(pack_tuple_data, METH_O, "serialize the give tuple message[tuple of bytes()]") \

/*
 * Given a tuple of bytes and None objects, join them into a
 * a single bytes object with sizes.
 */
static PyObject *
_pack_tuple_data(PyObject *tup)
{
	PyObject *rob;
	Py_ssize_t natts;
	Py_ssize_t catt;

	char *buf = NULL;
	char *bufpos = NULL;
	Py_ssize_t bufsize = 0;

	if (!PyTuple_Check(tup))
	{
		PyErr_Format(
			PyExc_TypeError,
			"pack_tuple_data requires a tuple, given %s",
			PyObject_TypeName(tup)
		);
		return(NULL);
	}
	natts = PyTuple_GET_SIZE(tup);
	if (natts == 0)
		return(PyBytes_FromString(""));

	/* discover buffer size and valid att types */
	for (catt = 0; catt < natts; ++catt)
	{
		PyObject *ob;
		ob = PyTuple_GET_ITEM(tup, catt);

		if (ob == Py_None)
		{
			bufsize = bufsize + 4;
		}
		else if (PyBytes_CheckExact(ob))
		{
			bufsize = bufsize + PyBytes_GET_SIZE(ob) + 4;
		}
		else
		{
			PyErr_Format(
				PyExc_TypeError,
				"cannot serialize attribute %d, expected bytes() or None, got %s",
				(int) catt, PyObject_TypeName(ob)
			);
			return(NULL);
		}
	}

	buf = malloc(bufsize);
	if (buf == NULL)
	{
		PyErr_Format(
			PyExc_MemoryError,
			"failed to allocate %d bytes of memory for packing tuple data",
			bufsize
		);
		return(NULL);
	}
	bufpos = buf;

	for (catt = 0; catt < natts; ++catt)
	{
		PyObject *ob;
		ob = PyTuple_GET_ITEM(tup, catt);
		if (ob == Py_None)
		{
			uint32_t attsize = 0xFFFFFFFFL; /* Indicates NULL */
			memcpy(bufpos, &attsize, 4);
			bufpos = bufpos + 4;
		}
		else
		{
			Py_ssize_t size = PyBytes_GET_SIZE(ob);
			uint32_t msg_size;
			if (size > 0xFFFFFFFE)
			{
				PyErr_Format(PyExc_OverflowError,
					"data size of %d is greater than attribute capacity",
					catt
				);
			}
			msg_size = local_ntohl((uint32_t) size);
			memcpy(bufpos, &msg_size, 4);
			bufpos = bufpos + 4;
			memcpy(bufpos, PyBytes_AS_STRING(ob), PyBytes_GET_SIZE(ob));
			bufpos = bufpos + PyBytes_GET_SIZE(ob);
		}
	}

	rob = PyBytes_FromStringAndSize(buf, bufsize);
	free(buf);
	return(rob);
}

/*
 * dst must be of PyTuple_Type with at least natts items slots.
 */
static int
_unpack_tuple_data(PyObject *dst, uint16_t natts, const char *data, Py_ssize_t data_len)
{
	PyObject *ob;
	uint16_t cnatt = 0;
	uint32_t attsize = 0;
	uint32_t position = 0;

	while (cnatt < natts)
	{
		/*
		 * Need enough data for the attribute size.
		 */
		if (position + 4 > data_len)
		{
			PyErr_Format(PyExc_ValueError,
				"not enough data available for attribute %d's size header: "
				"needed %d bytes, but only %lu remain at position %lu",
				cnatt, 4, data_len - position, position
			);
			return(-1);
		}

		memcpy(&attsize, data + position, 4);
		attsize = local_ntohl(attsize);
		position += 4;
		/*
		 * NULL.
		 */
		if (attsize == (uint32_t) 0xFFFFFFFFL)
		{
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(dst, cnatt, Py_None);
		}
		else
		{
			if ((position + attsize) < position)
			{
				/*
				 * Likely a "limitation" over the pure-Python version, *but*
				 * the message content size is limited to 0xFFFFFFFF-4 anyways,
				 * so it is unexpected for an attsize to cause wrap-around.
				 */
				PyErr_Format(PyExc_ValueError,
					"tuple data caused position (uint32_t) "
					"to wrap on attribute %d, position %lu + size %lu",
					cnatt, position, attsize
				);
				return(-1);
			}

			if (position + attsize > data_len)
			{
				PyErr_Format(PyExc_ValueError,
					"not enough data for attribute %d, size %lu, "
					"as only %lu bytes remain in message",
					cnatt, attsize, data_len - position
				);
				return(-1);
			}

			ob = PyBytes_FromStringAndSize(data + position, attsize);
			if (ob == NULL)
			{
				/*
				 * Probably an OOM error.
				 */
				return(-1);
			}
			PyTuple_SET_ITEM(dst, cnatt, ob);
			position += attsize;
		}

		cnatt++;
	}

	if (position != data_len)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple(D) message, %lu remaining "
			"bytes after processing %d attributes",
			data_len - position, cnatt
		);
		return(-1);
	}

	return(0);
}

static PyObject *
parse_tuple_message(PyObject *self, PyObject *args)
{
	PyObject *prerob, *rob, *temp_tup;
	PyObject *typ;
	const char *data;
	Py_ssize_t dlen = 0;
	uint16_t natts = 0;

	if (!PyArg_ParseTuple(args, "Oy#", &typ, &data, &dlen))
		return(NULL);

	if (dlen < 2)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple message: %d bytes is too small", dlen);
		return(NULL);
	}
	memcpy(&natts, data, 2);
	natts = local_ntohs(natts);

	prerob = PyTuple_New(natts);
	if (prerob == NULL)
		return(NULL);

	if (_unpack_tuple_data(prerob, natts, data+2, dlen-2) < 0)
	{
		Py_DECREF(prerob);
		return(NULL);
	}

	temp_tup = PyTuple_New(1);
	if (temp_tup == NULL)
	{
		Py_DECREF(prerob);
		return(NULL);
	}
	PyTuple_SET_ITEM(temp_tup, 0, prerob);

	rob = PyObject_CallObject(typ, temp_tup);
	Py_DECREF(temp_tup);
	return(rob);
}

static PyObject *
pack_tuple_data(PyObject *self, PyObject *tup)
{
	return(_pack_tuple_data(tup));
}
