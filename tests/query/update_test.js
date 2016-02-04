/**
 * @license
 * Copyright 2014 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
goog.setTestOnly();
goog.require('goog.testing.AsyncTestCase');
goog.require('goog.testing.jsunit');
goog.require('hr.db');
goog.require('lf.bind');
goog.require('lf.query.UpdateBuilder');
goog.require('lf.schema.DataStoreType');


/** @type {!goog.testing.AsyncTestCase} */
var asyncTestCase = goog.testing.AsyncTestCase.createAndInstall('Update');


/** @type {!lf.Database} */
var db;


function setUp() {
  asyncTestCase.waitForAsync('setUp');
  hr.db.connect({storeType: lf.schema.DataStoreType.MEMORY}).then(function(
      database) {
        db = database;
        asyncTestCase.continueTesting();
      }, fail);
}


function tearDown() {
  db.close();
}


/**
 * Tests that Update#exec() fails if set() has not been called first.
 */
function testExec_ThrowsMissingSet() {
  asyncTestCase.waitForAsync('testExec_ThrowsMissingSet');
  var employeeTable = db.getSchema().getEmployee();
  var query = new lf.query.UpdateBuilder(hr.db.getGlobal(), employeeTable);
  query.where(employeeTable.jobId.eq('dummyJobId'));
  query.exec().then(
      fail,
      function(e) {
        asyncTestCase.continueTesting();
      });
}


/**
 * Tests that Update#where() fails if where() has already been called.
 */
function testWhere_ThrowsAlreadyCalled() {
  var employeeTable = db.getSchema().getEmployee();
  var query = new lf.query.UpdateBuilder(hr.db.getGlobal(), employeeTable);

  var buildQuery = function() {
    var predicate = employeeTable.jobId.eq('dummyJobId');
    query.where(predicate).where(predicate);
  };

  assertThrows(buildQuery);
}


function testSet_ThrowsMissingBinding() {
  asyncTestCase.waitForAsync('testExec_ThrowsMissingBinding');
  var employeeTable = db.getSchema().getEmployee();
  var query = new lf.query.UpdateBuilder(hr.db.getGlobal(), employeeTable);
  query.set(employeeTable.minSalary, lf.bind(0));
  query.set(employeeTable.maxSalary, 20000);
  query.where(employeeTable.jobId.eq('dummyJobId'));
  query.exec().then(
      fail,
      function(e) {
        asyncTestCase.continueTesting();
      });
}


function testContext_Clone() {
  var j = db.getSchema().getJob();
  var query = /** @type {!lf.query.UpdateBuilder} */ (
      db.update(j).set(j.minSalary, lf.bind(1)).where(j.id.eq(lf.bind(0))));
  var context = query.getQuery();
  var context2 = context.clone();
  assertObjectEquals(context.table, context2.table);
  assertObjectEquals(context.set, context2.set);
  assertObjectEquals(context.where, context2.where);
  assertTrue(context2.clonedFrom == context);
  assertTrue(goog.getUid(context) != goog.getUid(context2));
  assertTrue(goog.getUid(context.where) != goog.getUid(context2.where));
}


function testNoWaitOnBinding() {
  var j = db.getSchema().getJob();
  var rows = [
    j.createRow({'id': '1', 'title': '1', 'minSalary': 1, 'maxSalary': 1}),
    j.createRow({'id': '2', 'title': '1', 'minSalary': 1, 'maxSalary': 2}),
    j.createRow({'id': '3', 'title': '1', 'minSalary': 1, 'maxSalary': 3})
  ];

  asyncTestCase.waitForAsync();
  db.insert().into(j).values(rows).exec().then(function() {
    var values = [[5, 6, '1'], [7, 8, '2'], [9, 10, '3']];
    var q = db.update(j)
        .set(j.minSalary, lf.bind(0))
        .set(j.maxSalary, lf.bind(1))
        .where(j.id.eq(lf.bind(2)));
    var promises = values.map(function(vals) {
      return q.bind(vals).exec();
    });
    return goog.Promise.all(promises);
  }).then(function() {
    return db.select().from(j).orderBy(j.id).exec();
  }).then(function(results) {
    assertEquals(5, results[0]['minSalary']);
    assertEquals(6, results[0]['maxSalary']);
    assertEquals(7, results[1]['minSalary']);
    assertEquals(8, results[1]['maxSalary']);
    assertEquals(9, results[2]['minSalary']);
    assertEquals(10, results[2]['maxSalary']);
    asyncTestCase.continueTesting();
  });
}
