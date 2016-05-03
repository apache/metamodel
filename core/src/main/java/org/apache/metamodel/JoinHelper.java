/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel;

import com.google.common.collect.Lists;
import org.apache.metamodel.data.*;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Join Execution and related methods.
 */
public abstract class JoinHelper {


    /**
     * Executes a simple nested loop join. The innerLoopDs will be copied in an in-memory dataset.
     *
     * @param outerLoopDs
     * @param innerLoopDs
     * @param filters
     * @return
     */
    public static InMemoryDataSet nestedLoopJoin( DataSet innerLoopDs,  DataSet outerLoopDs, Collection<FilterItem> filters){

        List<Row> innerRows = innerLoopDs.toRows();


        List<SelectItem> innerSelItems = Lists.newArrayList(innerLoopDs.getSelectItems());
        List<SelectItem> outerSelItems = Lists.newArrayList(outerLoopDs.getSelectItems());
        List<SelectItem> allItems = Lists.newArrayList(innerSelItems);
        allItems.addAll(outerSelItems);


        Set<FilterItem> filterAll = applicableFilters(filters, allItems);


        DataSetHeader jointHeader = joinHeader(outerLoopDs, innerLoopDs);

        List<Row> resultRows = Lists.newArrayList();
        for(Row outerRow: outerLoopDs){
            for(Row innerRow: innerRows){
                Row joinedRow =  joinRow(outerRow,innerRow,jointHeader);
                if(filterAll.isEmpty()|| filterAll.stream().allMatch(fi -> fi.accept(joinedRow))){
                    resultRows.add(joinedRow);
                }
            }
        }



        return new InMemoryDataSet(jointHeader,resultRows);
    }


    public static  Set<FilterItem> applicableFilters(Collection<FilterItem> filters, Collection<SelectItem> selectItemList) {

        Set<SelectItem> items = new HashSet<>(selectItemList);

        return filters.stream().filter( fi -> {
            Collection<SelectItem> fiSelectItems = Lists.newArrayList(fi.getSelectItem());
            Object operand = fi.getOperand();
            if(operand instanceof SelectItem){
                fiSelectItems.add((SelectItem) operand);
            }

            return items.containsAll(fiSelectItems);

        }).collect(Collectors.toSet());
    }





    /**
     * joins two datasetheader.
     * @param ds1 the headers for the left
     * @param ds2 the tright headers
     * @return
     */
    public static DataSetHeader joinHeader(DataSet ds1, DataSet ds2){
        List<SelectItem> joinedSelectItems = Lists.newArrayList(ds1.getSelectItems());
        joinedSelectItems.addAll(Lists.newArrayList(ds2.getSelectItems()));
        return  new CachingDataSetHeader(joinedSelectItems);


    }

    /**
     * Joins two rows into one.
     *
     * Consider parameter ordering to maintain backwards compatbility
     *
     * @param row1 the tuples, that will be on the left
     * @param row2 the tuples, that will be on the right
     * @param jointHeader
     * @return
     */
    public static Row joinRow(Row row1, Row row2, DataSetHeader jointHeader){
        Object[] joinedRow = new Object[row1.getValues().length + row2.getValues().length];

        System.arraycopy(row1.getValues(),0,joinedRow,0,row1.getValues().length);
        System.arraycopy(row2.getValues(),0,joinedRow,row1.getValues().length,row2.getValues().length);


        return new DefaultRow(jointHeader,joinedRow);


    }


}
