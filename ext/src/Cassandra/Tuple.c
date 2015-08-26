#include "php_cassandra.h"
#include "Tuple.h"

zend_class_entry *cassandra_tuple_ce = NULL;

int
php_cassandra_tuple_add(cassandra_tuple* tuple, zval* object TSRMLS_DC)
{
  if (zend_hash_next_index_insert(&tuple->values, (void*) &object, sizeof(zval*), NULL) == SUCCESS) {
    Z_ADDREF_P(object);
    tuple->size++;
    return 1;
  }

  return 0;
}

static int
php_cassandra_tuple_get(cassandra_tuple* tuple, ulong index, zval** zvalue)
{
  zval** value;

  if (zend_hash_index_find(&tuple->values, index, (void**) &value) == SUCCESS) {
    *zvalue = *value;
    return 1;
  }

  return 0;
}

/* {{{ Cassandra\Tuple::get(int) */
PHP_METHOD(Tuple, get)
{
  long key;
  cassandra_tuple* tuple = NULL;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l", &key) == FAILURE)
    return;

  tuple = (cassandra_tuple*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_tuple_get(tuple, (ulong) key, &value))
    RETURN_ZVAL(value, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Tuple::count() */
PHP_METHOD(Tuple, count)
{
  cassandra_tuple* tuple = (cassandra_tuple*) zend_object_store_get_object(getThis() TSRMLS_CC);

  RETURN_LONG(tuple->size);
}
/* }}} */

/* {{{ Cassandra\Tuple::__construct(string) */
PHP_METHOD(Tuple, __construct)
{
  char *type;
  int type_len;
  cassandra_tuple* tuple;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &type, &type_len) == FAILURE) {
    return;
  }

  tuple = (cassandra_tuple*) zend_object_store_get_object(getThis() TSRMLS_CC);
  tuple->size = 0;
  php_cassandra_value_type(type, &tuple->type TSRMLS_CC);
}


ZEND_BEGIN_ARG_INFO_EX(arginfo__construct, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, type)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_value, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_index, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, index)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_tuple_methods[] = {
  PHP_ME(Tuple, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Tuple, count, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Tuple, get, arginfo_index, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_tuple_handlers;

static void
php_cassandra_tuple_populate(cassandra_tuple* tuple, zval* array)
{
  HashPointer ptr;
  zval** current;

  zend_hash_get_pointer(&tuple->values, &ptr);
  zend_hash_internal_pointer_reset(&tuple->values);

  while (zend_hash_get_current_data(&tuple->values, (void**) &current) == SUCCESS) {
    if (add_next_index_zval(array, *current) == SUCCESS)
      Z_ADDREF_PP(current);
    else
      break;

    zend_hash_move_forward(&tuple->values);
  }

  zend_hash_set_pointer(&tuple->values, &ptr);
}

static HashTable*
php_cassandra_tuple_properties(zval *object TSRMLS_DC)
{
  zval* values;

  cassandra_tuple* tuple = (cassandra_tuple*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*     props = zend_std_get_properties(object TSRMLS_CC);

  MAKE_STD_ZVAL(values);
  array_init(values);

  php_cassandra_tuple_populate(tuple, values);

  zend_hash_update(props, "values", sizeof("values"), &values, sizeof(zval), NULL);

  return props;
}

static void
php_cassandra_tuple_free(void *object TSRMLS_DC)
{
  cassandra_tuple* tuple = (cassandra_tuple*) object;

  zend_hash_destroy(&tuple->values);
  zend_object_std_dtor(&tuple->zval TSRMLS_CC);

  efree(tuple);
}

static zend_object_value
php_cassandra_tuple_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_tuple *tuple;

  tuple = (cassandra_tuple*) emalloc(sizeof(cassandra_tuple));
  memset(tuple, 0, sizeof(cassandra_tuple));

  zend_hash_init(&tuple->values, 0, NULL, ZVAL_PTR_DTOR, 0);
  zend_object_std_init(&tuple->zval, class_type TSRMLS_CC);
#if ZEND_MODULE_API_NO >= 20100525
  object_properties_init(&tuple->zval, class_type);
#else
  zend_hash_copy(tuple->zval.properties, &class_type->default_properties, (copy_ctor_func_t) zval_add_ref, (void*) NULL, sizeof(zval*));
#endif

  retval.handle   = zend_objects_store_put(tuple,
                      (zend_objects_store_dtor_t) zend_objects_destroy_object,
                      php_cassandra_tuple_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_tuple_handlers;

  return retval;
}

void cassandra_define_Tuple(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Tuple", cassandra_tuple_methods);
  cassandra_tuple_ce = zend_register_internal_class(&ce TSRMLS_CC);
  memcpy(&cassandra_tuple_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_tuple_handlers.get_properties  = php_cassandra_tuple_properties;
  cassandra_tuple_ce->create_object = php_cassandra_tuple_new;
  cassandra_tuple_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
  }
