/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 *//*
 * Optimizations for protocol modules.
 *
 * This module.c file ties together other classified C source.
 * Each C-file describing the part of the protocol package that it
 * covers. It merely uses CPP includes to bring them into this
 * file and then uses some CPP macros to expand the definitions
 * in each file.
 */
#include <stdint.h>
#include <Python.h>
#include <structmember.h>

/*
 * buffer.c needs the message_types object from protocol.message_types.
 * Initialized in PyInit_optimized.
 */
static PyObject *message_types = NULL;
static long (*local_ntohl)(long) = NULL;
static short (*local_ntohs)(short) = NULL;


#include "typio.c"
#include "buffer.c"
#include "element3.c"


#define mFUNC(name, typ, doc) \
	{#name, (PyCFunction) name, typ, PyDoc_STR(doc)},
static PyMethodDef optimized_methods[] = {
	include_element3_functions
	include_typio_functions
	{NULL}
};
#undef mFUNC

static struct PyModuleDef optimized_module = {
   PyModuleDef_HEAD_INIT,
   "optimized",/* name of module */
   NULL,     /* module documentation, may be NULL */
   -1,       /* size of per-interpreter state of the module,
                or -1 if the module keeps state in global variables. */
   optimized_methods,
};

PyMODINIT_FUNC
PyInit_optimized(void)
{
	PyObject *mod;
	PyObject *msgtypes;
	PyObject *fromlist, *fromstr;
	long l = 1;

	mod = PyModule_Create(&optimized_module);
	if (mod == NULL)
		return(NULL);

/* cpp abuse; ready types */
#define mTYPE(name) \
	if (PyType_Ready(&name##_Type) < 0) \
		goto cleanup; \
	if (PyModule_AddObject(mod, #name, \
			(PyObject *) &name##_Type) < 0) \
		goto cleanup;

	/* buffer.c */
	include_buffer_types
#undef mTYPE

	if (((char *) &l)[0] == 1)
	{
		/* little */
		local_ntohl = swap_long;
		local_ntohs = swap_short;
	}
	else
	{
		/* big */
		local_ntohl = return_long;
		local_ntohs = return_short;
	}

	/*
	 * Get the message_types tuple to type "instantiation".
	 */
	fromlist = PyList_New(1);
	fromstr = PyUnicode_FromString("message_types");
	PyList_SetItem(fromlist, 0, fromstr);
	msgtypes = PyImport_ImportModuleLevel(
		"message_types",
		PyModule_GetDict(mod),
		PyModule_GetDict(mod),
		fromlist, 1
	);
	Py_DECREF(fromlist);
	if (msgtypes == NULL)
		goto cleanup;
	message_types = PyObject_GetAttrString(msgtypes, "message_types");
	Py_DECREF(msgtypes);

	if (!PyObject_IsInstance(message_types, (PyObject *) (&PyTuple_Type)))
	{
		PyErr_SetString(PyExc_RuntimeError,
			"local protocol.message_types.message_types is not a tuple object");
		goto cleanup;
	}

	return(mod);
cleanup:
	Py_DECREF(mod);
	return(NULL);
}
/*
 * vim: ts=3:sw=3:noet:
 */
