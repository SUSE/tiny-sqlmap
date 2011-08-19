/*
 * Copyright 2011 SUSE Linux Products GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.suse.lib.sqlmap;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Bo Maryniuk
 */
public class SQLParser {
    private static final int TYPE_STRING = 0;
    private static final int TYPE_INT = 1;
    private static final int TYPE_DATE = 2;
    private static final int TYPE_TIME = 3;
    private static final int TYPE_TIMESTAMP = 4;
    private static final int TYPE_DECIMAL = 5;
    private static final int TYPE_LONG = 6;
    private static final int TYPE_SHORT = 7;
    private static final int TYPE_BOOLEAN = 8;

    private final String query;
    private List<Value> values;
    private String preparedStatement;
    private HashMap<String, Integer> typemap;
    private SimpleDateFormat dateFormatter;
    private SimpleDateFormat timeFormatter;
    private SimpleDateFormat timestampFormatter;


    public static class Value {
        private String type;
        private String variable;
        private Object value;

        public Value(String type, String variable, Object value) {
            this.type = type;
            this.variable = variable;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public String getVariable() {
            return variable;
        }

        public Object getValue() {
            return value;
        }
    }


    public SQLParser(String query) {
        this.query = query;
        this.values = new ArrayList<Value>();

        this.typemap = new HashMap<String, Integer>();
        this.typemap.put("int", SQLParser.TYPE_INT);
        this.typemap.put("short", SQLParser.TYPE_SHORT);
        this.typemap.put("long", SQLParser.TYPE_LONG);
        this.typemap.put("string", SQLParser.TYPE_STRING);
        this.typemap.put("decimal", SQLParser.TYPE_DECIMAL);
        this.typemap.put("date", SQLParser.TYPE_DATE);
        this.typemap.put("time", SQLParser.TYPE_TIME);
        this.typemap.put("timestamp", SQLParser.TYPE_TIMESTAMP);
        this.typemap.put("boolean", SQLParser.TYPE_BOOLEAN);

        this.dateFormatter = new SimpleDateFormat("yyyy.MM.dd");
        this.timeFormatter = new SimpleDateFormat("HH:mm:ss");
        this.timestampFormatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
    }


    public SQLParser parse(Map<?, ?> params) throws Exception {
        this.preparedStatement = null;
        this.values.clear();
        String[] leftTokens = this.query.split("\\{");
        for (int i = 0; i < leftTokens.length; i++) {
            String token = leftTokens[i];
            if (token.contains("}")) {
                token = token.split("\\}")[0];
                if (token.contains(":") && token.indexOf(":") > 0) {
                    String[] typeAndVariable = token.split(":");
                    this.values.add(new Value(typeAndVariable[0], typeAndVariable[1], params.get(typeAndVariable[1])));
                } else {
                    throw new Exception("Illegal syntax: " + token);
                }
            }
        }

        String prepQuery = this.query;
        for (int i = 0; i < values.size(); i++) {
            SQLParser.Value value = values.get(i);
            prepQuery = prepQuery.replace(String.format("{%s:%s}", value.getType(), value.getVariable()), "?");
        }

        // At this point query should be already fully prepared, i.e. all variables replaced with a questionmark.
        if (prepQuery.contains("{") || prepQuery.contains("}")) {
            throw new Exception("Query was not fully prepared: " + prepQuery);
        }

        this.preparedStatement = prepQuery;

        return this;
    }



    /**
     * Prepare SQL statement for execution.
     * 
     * @param connection
     * @return
     * @throws SQLException
     * @throws ParseException
     */
    public PreparedStatement prepare(Connection connection) throws SQLException, ParseException {
        PreparedStatement statement = connection.prepareStatement(this.preparedStatement);
        for (int i = 0; i < values.size(); i++) {
            Value value = values.get(i);
            Integer type = this.typemap.get(value.getType());
            switch (type == null ? SQLParser.TYPE_STRING : type) {
                case SQLParser.TYPE_DATE:
                    statement.setDate(i + 1, (Date) (value.getValue() instanceof java.util.Date
                                                     || value.getValue() instanceof java.sql.Date
                                                     ? value.getValue()
                                                     : this.dateFormatter.parse((String) value.getValue())));
                    break;

                case SQLParser.TYPE_TIME:
                    statement.setTime(i + 1, new Time(((java.util.Date) (value.getValue() instanceof java.util.Date
                                                                         || value.getValue() instanceof java.sql.Date
                                                                         || value.getValue() instanceof java.sql.Time
                                                                         || value.getValue() instanceof java.sql.Timestamp
                                                                         ? value.getValue()
                                                                         : this.timeFormatter.parse((String) value.getValue()))).getTime()));
                    break;

                case SQLParser.TYPE_TIMESTAMP:
                    statement.setTimestamp(i + 1, new Timestamp(((java.util.Date) (value.getValue() instanceof java.util.Date
                                                                                   || value.getValue() instanceof java.sql.Date
                                                                                   ? value.getValue()
                                                                                   : this.timestampFormatter.parse((String) value.getValue()))).getTime()));
                    break;

                case SQLParser.TYPE_DECIMAL:
                    statement.setBigDecimal(i + 1, (BigDecimal) (value.getValue() instanceof BigDecimal
                                                                 ? value.getValue()
                                                                 : new BigDecimal((String) value.getValue())));
                    break;

                case SQLParser.TYPE_INT:
                    statement.setInt(i + 1, (Integer) ((value.getValue() instanceof Integer)
                                                       ? value.getValue()
                                                       : new Integer(value.getValue().toString())));
                    break;

                case SQLParser.TYPE_SHORT:
                    statement.setShort(i + 1, (Short) ((value.getValue() instanceof Short)
                                                       ? value.getValue()
                                                       : new Short(value.getValue().toString())));
                    break;

                case SQLParser.TYPE_LONG:
                    statement.setLong(i + 1, (Long) ((value.getValue() instanceof Long)
                                                     ? value.getValue()
                                                     : new Long(value.getValue().toString())));
                    break;

                case SQLParser.TYPE_BOOLEAN:
                    statement.setBoolean(i + 1, (Boolean) ((value.getValue() instanceof Boolean)
                                                           ? value.getValue()
                                                           : Boolean.valueOf(value.getValue().toString())));
                    break;

                default: // String
                    statement.setString(i + 1, (String) value.getValue());
                    break;
            }
        }

        return statement;
    }
}
