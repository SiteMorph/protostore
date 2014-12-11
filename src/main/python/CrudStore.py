import copy
import string
import uuid

import MySQLdb as db

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2



class CrudIterator :

  def __init__(self, prototype, descriptor, cursor) :
    self.prototype = prototype
    self.descriptor = descriptor
    self.cursor = cursor
    self.offset = 0
    self.rowCount = cursor.rowcount


  def hasNext(self) :
      return self.offset < self.rowCount

  def next(self) :
    row = self.cursor.fetchone()
    self.offset += 1
    result = message.Message
    for field in self.descriptor.fields :
      setattr(result, field.name, row[field.index])
    return result

  def close(self):
    if None != self.cursor :
      self.cursor.close()


class CrudStore :

  def __init__(self, prototype, descriptor, tableName, urnField,
      sortField = None, ascending = False) :
    self.prototype = prototype
    self.descriptor = descriptor
    self.tableName = tableName
    self.urnField = urnField
    self.sortField = sortField
    self.ascending = ascending


  def setDatabase(self, db) :
    self.db = db

  def create(self, message):
    if not hasattr(message, self.urnField) :
      setattr(message, self.urnField, str(uuid.uuid4()))
    insert = self.buildCreate(message)
    cursor = self.db.cursor()
    cursor.execute(insert)
    cursor.close()


  def readUrn(self, urn) :
    select = self.buildStatement(self.urnField, urn)
    cursor = self.db.cursor()
    cursor.execute(select)
    return CrudIterator(self.prototype,  self.descriptor, cursor)

  def readIndex(self, indexField, indexValue):
    select = self.buildStatement(indexField, indexValue)
    # print select
    cursor = self.db.cursor()
    cursor.execute(select)
    return CrudIterator(self.prototype, self.descriptor, cursor)

  def readAll(self) :
    select = self.buildStatement(None, None)
    cursor = self.db.cursor()
    cursor.execute(select)
    return CrudIterator(self.prototype, self.descriptor, cursor)

  def delete(self, message):
    delete = self.buildDelete(message)
    cursor = self.db.cursor()
    cursor.execute(delete)
    cursor.close()

  def buildStatement(self, matchField, matchValue) :
    result = 'SELECT '
    for field in self.descriptor.fields :
      result += field.name
      result += ','
    result = result[:-1]
    result += ' FROM '
    result += self.tableName
    if matchField != None :
      result += ' WHERE '
      result += matchField
      result += ' = "'
      result += matchValue
      result += '" '
    if self.sortField != None :
      result += ' ORDER BY '
      result += self.sortField
      if (self.ascending) :
        result += ' ASC '
      else :
        result += ' DESC '
    return result

  def buildCreate(self, message):
    result = 'INSERT INTO '
    result += self.tableName
    result += '('
    for field in self.descriptor.fields :
      result += field.name
      result += ','
    result = result[:-1]
    result += ') VALUES ('
    for field in self.descriptor.fields :
      result += self.wrapValue(message, field)
      result += ','
    result = result[:-1]
    result += ')'
    return result

  def buildDelete(self, message) :
    result = 'DELETE FROM '
    result += self.tableName
    result += ' WHERE '
    result += self.urnField
    result += ' = '
    result += self.quoteString(getattr(message, self.urnField))
    return result

  def wrapValue(self, container, fieldDescriptor) :
    if hasattr(container, fieldDescriptor.name) :
      if fieldDescriptor.type == descriptor.FieldDescriptor.TYPE_STRING :
        return self.quoteString(getattr(container, fieldDescriptor.name))
      elif fieldDescriptor.type == descriptor.FieldDescriptor.TYPE_BOOL :
        if getattr(container, fieldDescriptor.name) :
          return 'TRUE'
        else :
          return 'FALSE'
      else :
        numeric = getattr(container, fieldDescriptor.name)
        if None == numeric :
          return 'NULL'
        else :
          return str(numeric)
    else :
      return 'NULL'

  def quoteString(self, content) :
    if None == content :
      return 'NULL'
    result = '"'
    result += content.replace('"', '\\"')
    result += '"'
    return result
