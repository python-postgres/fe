/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 */
#define include_typio_functions \
	mFUNC(process_tuple, METH_VARARGS, \
		"process the items in the second argument " \
		"with the corresponding items in the first argument.") \
	mFUNC(int2_pack, METH_O, "PyInt to serialized, int2") \
	mFUNC(int2_unpack, METH_O, "PyInt from serialized, int2") \
	mFUNC(int4_pack, METH_O, "PyInt to serialized, int4") \
	mFUNC(int4_unpack, METH_O, "PyInt from serialized, int4") \
	mFUNC(swap_int2_pack, METH_O, "PyInt to swapped serialized, int2") \
	mFUNC(swap_int2_unpack, METH_O, "PyInt from swapped serialized, int2") \
	mFUNC(swap_int4_pack, METH_O, "PyInt to swapped serialized, int4") \
	mFUNC(swap_int4_unpack, METH_O, "PyInt from swapped serialized, int4") \
	mFUNC(uint2_pack, METH_O, "PyInt to serialized, int2") \
	mFUNC(uint2_unpack, METH_O, "PyInt from serialized, int2") \
	mFUNC(uint4_pack, METH_O, "PyInt to serialized, int4") \
	mFUNC(uint4_unpack, METH_O, "PyInt from serialized, int4") \
	mFUNC(swap_uint2_pack, METH_O, "PyInt to swapped serialized, int2") \
	mFUNC(swap_uint2_unpack, METH_O, "PyInt from swapped serialized, int2") \
	mFUNC(swap_uint4_pack, METH_O, "PyInt to swapped serialized, int4") \
	mFUNC(swap_uint4_unpack, METH_O, "PyInt from swapped serialized, int4") \

/*
 * Define the swap functionality.
 */
#define swap2(CP) do{register char c; \
	c=CP[1];CP[1]=CP[0];CP[0]=c;\
}while(0)
#define swap4(P) do{register char c; \
	c=P[3];P[3]=P[0];P[0]=c;\
	c=P[2];P[2]=P[1];P[1]=c;\
}while(0)
#define swap8(P) do{register char c; \
	c=P[7];P[7]=P[0];P[0]=c;\
	c=P[6];P[6]=P[1];P[1]=c;\
	c=P[5];P[5]=P[2];P[2]=c;\
	c=P[4];P[4]=P[3];P[3]=c;\
}while(0)

static short
swap_short(short s)
{
	swap2(((char *) &s));
	return(s);
}

static short
return_short(short s)
{
	return(s);
}

static long
swap_long(long l)
{
	swap4(((char *) &l));
	return(l);
}

static long
return_long(long l)
{
	return(l);
}

static PyObject *
int2_pack(PyObject *self, PyObject *arg)
{
	long l;
	short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (l > SHORT_MAX || l < SHORT_MIN)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%d' overflows int2", l
		);
		return(NULL);
	}

	s = (short) l;
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}
static PyObject *
swap_int2_pack(PyObject *self, PyObject *arg)
{
	long l;
	short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	if (l > SHORT_MAX || l < SHORT_MIN)
	{
		PyErr_SetString(PyExc_OverflowError, "long too big or small for int2");
		return(NULL);
	}

	s = (short) l;
	swap2(((char *) &s));
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}

static PyObject *
int2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	short *i;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	len = PyBytes_Size(arg);
	if (len != 2)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for int2_unpack");
		return(NULL);
	}

	i = (short *) c;
	l = (long) *i;
	rob = PyLong_FromLong(l);
	return(rob);
}
static PyObject *
swap_int2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	short s;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	len = PyBytes_Size(arg);
	if (len != 2)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for swap_int2_unpack");
		return(NULL);
	}

	s = *((short *) c);
	swap2(((char *) &s));
	l = (long) s;
	rob = PyLong_FromLong(l);
	return(rob);
}

static PyObject *
int4_pack(PyObject *self, PyObject *arg)
{
	long l;
	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	return(PyBytes_FromStringAndSize((const char *) &l, 4));
}
static PyObject *
swap_int4_pack(PyObject *self, PyObject *arg)
{
	long l;
	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	swap4(((char *) &l));
	return(PyBytes_FromStringAndSize((const char *) &l, 4));
}

static PyObject *
int4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	long l;
	Py_ssize_t len;

	len = PyBytes_Size(arg);
	if (len != 4)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for int4_unpack");
		return(NULL);
	}
	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);
	l = *((long *) c);

	return(PyLong_FromLong(l));
}
static PyObject *
swap_int4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	long l;
	Py_ssize_t len;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	len = PyBytes_Size(arg);
	if (len != 4)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for swap_int4_unpack");
		return(NULL);
	}

	l = *((long *) c);
	swap4(((char *) &l));
	return(PyLong_FromLong(l));
}

static PyObject *
uint2_pack(PyObject *self, PyObject *arg)
{
	long l;
	unsigned short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (l > USHORT_MAX || l < 0)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%d' overflows uint2", l
		);
		return(NULL);
	}

	s = (unsigned short) l;
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}
static PyObject *
swap_uint2_pack(PyObject *self, PyObject *arg)
{
	long l;
	unsigned short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (l > USHORT_MAX || l < 0)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%d' overflows uint2", l
		);
		return(NULL);
	}

	s = (unsigned short) l;
	swap2(((char *) &s));
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}

static PyObject *
uint2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	unsigned short *i;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	len = PyBytes_Size(arg);
	if (len != 2)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for uint2_unpack");
		return(NULL);
	}

	i = (unsigned short *) c;
	l = (long) *i;
	rob = PyLong_FromLong(l);
	return(rob);
}
static PyObject *
swap_uint2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	unsigned short s;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	len = PyBytes_Size(arg);
	if (len != 2)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for swap_uint2_unpack");
		return(NULL);
	}

	s = *((short *) c);
	swap2(((char *) &s));
	l = (long) s;
	rob = PyLong_FromLong(l);
	return(rob);
}

static PyObject *
uint4_pack(PyObject *self, PyObject *arg)
{
	unsigned long l;
	l = PyLong_AsUnsignedLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	return(PyBytes_FromStringAndSize((const char *) &l, 4));
}
static PyObject *
swap_uint4_pack(PyObject *self, PyObject *arg)
{
	unsigned long l;
	l = PyLong_AsUnsignedLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	swap4(((char *) &l));
	return(PyBytes_FromStringAndSize((const char *) &l, 4));
}

static PyObject *
uint4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	unsigned long l;
	Py_ssize_t len;

	len = PyBytes_Size(arg);
	if (len != 4)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for uint4_unpack");
		return(NULL);
	}
	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);
	l = *((unsigned long *) c);

	return(PyLong_FromUnsignedLong(l));
}
static PyObject *
swap_uint4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	unsigned long l;
	Py_ssize_t len;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	len = PyBytes_Size(arg);
	if (len != 4)
	{
		PyErr_SetString(PyExc_ValueError, "invalid size of data for swap_uint4_unpack");
		return(NULL);
	}

	l = *((unsigned long *) c);
	swap4(((char *) &l));
	return(PyLong_FromUnsignedLong(l));
}


/*
 * process the tuple with the associated callables while
 * calling the third object in cases of failure to generalize the exception.
 */
static PyObject *
process_tuple(PyObject *self, PyObject *args)
{
	PyObject *tup, *procs, *fail, *rob;
	Py_ssize_t len, i;

	if (!PyArg_ParseTuple(args, "OOO", &procs, &tup, &fail))
		return(NULL);

	if (!PyTuple_CheckExact(procs))
	{
		PyErr_SetString(
			PyExc_TypeError,
			"process_tuple requires a tuple as its first argument"
		);
		return(NULL);
	}

	if (!PyTuple_Check(tup))
	{
		PyErr_SetString(
			PyExc_TypeError,
			"process_tuple requires a tuple as its second argument"
		);
		return(NULL);
	}

	len = PyTuple_GET_SIZE(tup);

	if (len != PyTuple_GET_SIZE(procs))
	{
		PyErr_Format(
			PyExc_ValueError,
			"inconsistent items, %d processors and %d objects",
			len,
			PyTuple_GET_SIZE(procs)
		);
		return(NULL);
	}
	rob = PyTuple_New(len);

	for (i = 0; i < len; ++i)
	{
		PyObject *p, *o, *ot, *r;
		/*
		 * If it's Py_None, that means it's NULL. No processing necessary.
		 */
		o = PyTuple_GET_ITEM(tup, i);
		if (o == Py_None)
		{
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(rob, i, Py_None);
			continue;
		}

		p = PyTuple_GET_ITEM(procs, i);
		/*
		 * Temp tuple for applying *args to p.
		 */
		ot = PyTuple_New(1);
		PyTuple_SET_ITEM(ot, 0, o);
		Py_INCREF(o);

		r = PyObject_CallObject(p, ot);
		Py_DECREF(ot);
		if (r == NULL)
		{
			/*
			 * Exception from p(*ot)
			 */
			Py_DECREF(rob);
			rob = NULL;

			/*
			 * Don't trap BaseException's.
			 */
			if (PyErr_ExceptionMatches(PyExc_Exception))
			{
				PyObject *failargs, *failedat;
				PyObject *exc, *val, *tb;
				PyObject *oldexc, *oldval, *oldtb;

				/* Store exception to set context after handler. */
				PyErr_Fetch(&oldexc, &oldval, &oldtb);
				PyErr_NormalizeException(&oldexc, &oldval, &oldtb);

				failedat = PyLong_FromSsize_t(i);
				if (failedat != NULL)
				{
					failargs = PyTuple_New(3);
					if (failargs != NULL)
					{
						/* args for the exception "handler" */
						PyTuple_SET_ITEM(failargs, 0, procs);
						Py_INCREF(procs);
						PyTuple_SET_ITEM(failargs, 1, tup);
						Py_INCREF(tup);
						PyTuple_SET_ITEM(failargs, 2, failedat);

						r = PyObject_CallObject(fail, failargs);
						Py_DECREF(failargs);
						if (r != NULL)
						{
							PyErr_SetString(PyExc_RuntimeError,
								"process_tuple exception handler failed to raise"
							);
							Py_DECREF(r);
						}
					}
					else
					{
						Py_DECREF(failedat);
					}
				}

				PyErr_Fetch(&exc, &val, &tb);
				PyErr_NormalizeException(&exc, &val, &tb);

				/*
				 * Reference BaseException here as the condition is merely
				 * *validating* that SetContext can be used.
				 */
				if (val != NULL && PyObject_IsInstance(val, PyExc_BaseException))
				{
					/* Steals oldval reference */
					PyException_SetContext(val, oldval);
					Py_XDECREF(oldexc);
					Py_XDECREF(oldtb);
					PyErr_Restore(exc, val, tb);
				}
				else
				{
					/*
					 * Fetch & NormalizeException failed somehow.
					 * Use the old exception...
					 */
					PyErr_Restore(oldexc, oldval, oldtb);
					Py_XDECREF(exc);
					Py_XDECREF(val);
					Py_XDECREF(tb);
				}
			}

			/*
			 * Break out of loop to return(NULL);
			 */
			break;
		}
		PyTuple_SET_ITEM(rob, i, r);
	}

	return(rob);
}
