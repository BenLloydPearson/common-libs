package com.gravity.utilities.cache

import org.apache.commons.pool.impl.{GenericObjectPool, GenericKeyedObjectPool}
import org.apache.commons.pool.KeyedPoolableObjectFactory

trait Locking {

  val loaderLockPool: GenericKeyedObjectPool = new GenericKeyedObjectPool(new KeyedPoolableObjectFactory() {
    def makeObject(key: Object): Object = new Object()

    def activateObject(key: Object, obj: Object) {}

    def passivateObject(key: Object, obj: Object) {}

    def validateObject(key: Object, obj: Object) = true

    def destroyObject(key: Object, obj: Object) {}
  }, 1, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, -1)

  def withLock[T](key: String)(work: => T): T = withObjectLock(key.asInstanceOf[Object])(work)

  def withLock[T](key: Long)(work: => T): T = withObjectLock(key.asInstanceOf[Object])(work)

  private def withObjectLock[T](key: Object)(work: => T): T = {
    val lock = loaderLockPool.borrowObject(key)
    try {
      work
    } finally {
      loaderLockPool.returnObject(key, lock)
    }
  }

}