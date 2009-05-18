/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 *//*
 * element3 optimizations.
 */
#define include_element3_functions \
	mFUNC(parse_tuple_message, METH_VARARGS, "parse the given tuple data into a tuple of raw data") \

static PyObject *
parse_tuple_message(PyObject *self, PyObject *args)
{
	PyObject *rob;
	PyObject *ob;
	PyObject *typ;
	const char *data;
	Py_ssize_t dlen = 0;
	uint16_t cnatt = 0, natts = 0;
	uint32_t attsize = 0;
	uint32_t position = 0;

	if (!PyArg_ParseTuple(args, "Oy#", &typ, &data, &dlen))
		return(NULL);

	/*
	 * Validate that the given "typ" is in fact a PyTuple_Type subtype.
	 */
	if (typ != Py_None)
	{
		if (!PyObject_IsSubclass(typ, (PyObject *) &PyTuple_Type))
		{
			const char *typname = "<not-a-type>";
			if (PyObject_IsInstance(
				(PyObject *) typ->ob_type,
				(PyObject *) &PyType_Type
			))
			{
				typname = ((PyTypeObject *) typ)->tp_name;
			}

			PyErr_Format(
				PyExc_TypeError,
				"cannot instantiate wire tuple into a non-tuple subtype: %s",
				typname
			);
			return(NULL);
		}
	}
	else
	{
		typ = (PyObject *) &PyTuple_Type;
	}

	if (dlen < 2)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple message: %d bytes is too small", dlen);
		return(NULL);
	}
	natts = ntohs(*((uint16_t *) (data)));

	/*
	 * FEARME: A bit much for saving a reallocation/copy?
	 *
	 * This is expected to be used as a classmethod on a tuple subtype that
	 * has *no* additional attributes.
	 *
	 * If the subtype has a custom __new__ routine, this could
	 * be problematic, but it *should* only lead to AttributeErrors.
	 */
	rob = ((PyTypeObject *) typ)->tp_alloc((PyTypeObject *) typ, natts); 
	if (rob == NULL)
	{
		return(NULL);
	}

	position += 2;
	while (cnatt < natts)
	{
		/*
		 * Need enough data for the attribute size.
		 */
		if (position + 4 > dlen)
		{
			PyErr_Format(PyExc_ValueError,
				"not enough data available for attribute %d's size header: "
				"needed %d bytes, but only %lu remain at position %lu",
				cnatt, 4, dlen - position, position
			);
			goto cleanup;
		}

		attsize = ntohl(*((uint32_t *) (data + position)));
		position += 4;
		/*
		 * NULL.
		 */
		if (attsize == 0xFFFFFFFFL)
		{
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(rob, cnatt, Py_None);
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
				goto cleanup;
			}

			if (position + attsize > dlen)
			{
				PyErr_Format(PyExc_ValueError,
					"not enough data for attribute %d, size %lu, "
					"as only %lu bytes remain in message",
					cnatt, attsize, dlen - position
				);
				goto cleanup;
			}

			ob = PyBytes_FromStringAndSize(data + position, attsize);
			if (ob == NULL)
			{
				/*
				 * Probably an OOM error.
				 */
				goto cleanup;
			}
			PyTuple_SET_ITEM(rob, cnatt, ob);
			position += attsize;
		}

		cnatt++;
	}

	if (position != dlen)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple message, %lu remaining "
			"bytes after processing %d attributes",
			dlen - position, cnatt
		);
		goto cleanup;
	}

	return(rob);

cleanup:
	Py_DECREF(rob);
	return(NULL);
}
