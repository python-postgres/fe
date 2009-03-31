/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 *//*
 * optimizations for postgresql.python package modules.
 */
#include <Python.h>
#include <structmember.h>

static PyObject *
rsetattr(PyObject *self, PyObject *args)
{
	PyObject *ob, *attr, *val;

	if (!PyArg_ParseTuple(args, "OOO", &attr, &val, &ob))
		return(NULL);

	if (PyObject_SetAttr(ob, attr, val) < 0)
		return(NULL);

	Py_INCREF(ob);
	return(ob);
}

/*
 * Override the functools.Composition __call__.
 */
static PyObject *
compose(PyObject *self, PyObject *args)
{
	Py_ssize_t i, len;
	PyObject *rob, *argt, *seq, *x;

	if (!PyArg_ParseTuple(args, "OO", &seq, &rob))
		return(NULL);

	Py_INCREF(rob);
	if (PyObject_IsInstance(seq, (PyObject *) &PyTuple_Type))
	{
		len = PyTuple_GET_SIZE(seq);
		for (i = 0; i < len; ++i)
		{
			x = PyTuple_GET_ITEM(seq, i);
			argt = PyTuple_New(1);
			PyTuple_SET_ITEM(argt, 0, rob);
			rob = PyObject_CallObject(x, argt);
			Py_DECREF(argt);
			if (rob == NULL)
				break;
		}
	}
	else if (PyObject_IsInstance(seq, (PyObject *) &PyList_Type))
	{
		len = PyList_GET_SIZE(seq);
		for (i = 0; i < len; ++i)
		{
			x = PyList_GET_ITEM(seq, i);
			argt = PyTuple_New(1);
			PyTuple_SET_ITEM(argt, 0, rob);
			rob = PyObject_CallObject(x, argt);
			Py_DECREF(argt);
			if (rob == NULL)
				break;
		}
	}
	else
	{
		/*
		 * Arbitrary sequence.
		 */
		len = PySequence_Length(seq);
		for (i = 0; i < len; ++i)
		{
			x = PySequence_GetItem(seq, i);
			argt = PyTuple_New(1);
			PyTuple_SET_ITEM(argt, 0, rob);
			rob = PyObject_CallObject(x, argt);
			Py_DECREF(x);
			Py_DECREF(argt);
			if (rob == NULL)
				break;
		}
	}

	return(rob);
}

static PyMethodDef optimized_methods[] = {
	{"rsetattr", rsetattr, METH_VARARGS,
		PyDoc_STR(
			"rsetattr(attr, val, ob) set the attribute to the value *and* return `ob`."
		),
	},
	{"compose", compose, METH_VARARGS,
		PyDoc_STR(
			"given a sequence of callables, "
			"and an argument for the first call, compose the result."
		),
	},
	{NULL}
};

static struct PyModuleDef optimized_module = {
   PyModuleDef_HEAD_INIT,
   "optimized",	/* name of module */
   NULL,				/* module documentation, may be NULL */
   -1,				/* size of per-interpreter state of the module,
							or -1 if the module keeps state in global variables. */
   optimized_methods,
};

PyMODINIT_FUNC
PyInit_optimized(void)
{
	return(PyModule_Create(&optimized_module));
}
/*
 * vim: ts=3:sw=3:noet:
 */
