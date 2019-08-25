import { db } from '../utils/knex-db';
import { DtlRequest, DtlSelect, DtlOrderby, DtlWhereGroup, DtlWhereCondition } from './types';
import { isNull } from 'util';
import { DtlWhereConditionOperation } from './types/dtl-where-condition-operation.enum';
import { DtlHaving } from './types/dtl-having.type';

/**
 * Parse and create a database query
 * @Return : JSON file containing database response
 */
export class QueryBuilder {

    request: DtlRequest;
    query: any;
    message: string;

    constructor(request: DtlRequest) {
        this.request = request;
        this.checkRequest();
        this.message = "Request:";
        this.build();
    }

    private checkRequest() {
        if (!this.request) {
            // Throw error: No table found in the request
            throw new Error("No request defined");
        } else if (!this.request.table) {
            // Throw error: No table found in the request
            throw new Error("No table found in the request");
        } else {
            return true;
        }
    }

    private build(): void {

        this.query = db(this.request.table);

        this.parseSelect();

        if (this.request.where) {
            this.query = this.parseWhere(this.query, this.request.where);
        }

        if (this.request.orderBy) {
            this.request.orderBy.forEach((orderBy: DtlOrderby) => {
                const column = orderBy.column;
                const order = orderBy.sort;
                this.query = this.query.orderBy(column, order);
            });
        }

        if (this.request.limit) {
            if (this.request.limit) {
                this.query = this.query.limit(this.request.limit);
            }
        }
        if (this.request.offset) {
            if (this.request.offset) {
                this.query = this.query.offset(this.request.offset);
            }
        }
    }

    private parseSelect() {
        if (this.request.select) {
            if (this.request.select.length == 0) {
                this.query = this.query.select("*");
            } else {
                // Detect if groupBy is to be used.
                let isGrouped: boolean;
                this.request.select.forEach((select: DtlSelect) => {
                    isGrouped = (isGrouped || (select.aggregate != null));
                });
                this.request.select.forEach((select: DtlSelect) => {
                    let field = null;
                    let originalField = null;
                    if (select.field != null) {
                        if (select.as != null) {
                            field = (select.field + " as " + select.as);
                            originalField = select.as;
                        } else {
                            if (isGrouped && select.aggregate) {
                                field = select.field + " as " + select.field + '_' + select.aggregate;
                            } else {
                                field = select.field;
                            }
                            originalField = select.field;
                        }
                        // fonction pour group, et ajouter
                        switch (select.aggregate) {
                            case 'count':
                                this.query = this.query.count(field);
                                break;
                            case 'min':
                                this.query = this.query.min(field);
                                break;
                            case 'max':
                                this.query = this.query.max(field);
                                break;
                            case 'sum':
                                this.query = this.query.sum(field);
                                break;
                            case 'avg':
                                this.query = this.query.avg(field);
                                break;
                            default:
                                if (isGrouped) {
                                    if (originalField != null) {
                                        this.query = this.query.select(field);
                                        this.query = this.query.groupBy(originalField);
                                    }
                                } else {
                                    this.query = this.query.select(field);
                                }
                                break;
                        }
                    }
                });
            }
        }
    }

    private parseWhere(query: any, whereGroup: DtlWhereGroup) {
        if (whereGroup) {
            const link = whereGroup.link;
            // Go through all DtlWhereGroup objects in conditions:
            const conditions: (DtlWhereGroup | DtlWhereCondition)[] = [];
            if (!isNull(link)) {
                whereGroup.conditions.forEach(newWhereGroup => {
                    conditions.push(newWhereGroup);
                });
                // Parse each new DtlWhereGroup:
                conditions.forEach(newWhereGroup => {
                    // Seperate the clauses (field, operation, value) from the newWhereGroup (link, objects)
                    query = this.parseNewWhereGroup(newWhereGroup, link, query);
                });
            }
        }
        return query;
    }

    // Recursive transform whereObject into query
    public parseNewWhereGroup(newWhereGroup: any, operator: string, query: any) {

        // Check if DtlWhereGroup is another DtlWhereGroup
        if (this.identifyWhereGroup(newWhereGroup) == "unknown") {
            if (operator == "or") {
                // Recursive, pass new DtlWhereGroup objects through main parseWhere();
                query.orWhere((query: any) => { this.parseWhere(query, newWhereGroup); });
            } else {
                query.where((query: any) => { this.parseWhere(query, newWhereGroup); });
            }

            // Check if DtlWhereGroup is a clause: (field, operation, value)
        } else if (this.identifyWhereGroup(newWhereGroup) == "condition") {
            const clause = this.formatClause(newWhereGroup.field, newWhereGroup.operation, newWhereGroup.value);
            (operator == "or") ? query.orWhere(clause.field, clause.operation, clause.value) : query.where(clause.field, clause.operation, clause.value);
        }
        return query;
    }

    /**
     * Checks for operation,
     * uses RegEx to determine the operator
     * @param whereObject : DtlWhereGroup
     */
    public getOperation(whereObject: DtlWhereGroup) {
        if (whereObject != null) {
            return whereObject.link;
        }
        return null;
    }
    /**
     * Check if object is (condition or whereObject)
     * @param newWhereGroup
     */
    public identifyWhereGroup(newWhereGroup: DtlWhereGroup) {
        if (newWhereGroup != null) {
            const keys = Object.keys(newWhereGroup);
            return (keys[0] == "field") ? "condition" : "unknown";
        }
    }

    // Adds having to object
    public queryHaving(query: any, having: DtlHaving[]) {
        having.forEach((havObj: DtlHaving) => {
            if (havObj.operation) {
                query = query.having(this.getColumnName(havObj), havObj.operation, havObj.value);
            }
        });
        return query;
    }
    // checks if alias field exists in column name
    public getColumnName(column: DtlHaving) {
        return column.colonne;
    }

    // reformats operation into readable format for knex
    public formatClause(field: string, operation: string, value: string) {
        // return object{field, operator, value}
        const clause: DtlWhereCondition = <DtlWhereCondition>{
            value,
            field
        };
        // Possibility to change this to enum:
        switch (operation) {

            case 'LIKE':
                clause.operation = DtlWhereConditionOperation.LIKE;
                break;
            case '>':
                clause.operation = DtlWhereConditionOperation.GT;
                break;
            case '<':
                clause.operation = DtlWhereConditionOperation.LT;
                break;
            case '=':
                clause.operation = DtlWhereConditionOperation.EQ;
                break;
            case 'not':
                clause.operation = DtlWhereConditionOperation.NOT;
                break;
            case 'startswith':
                clause.operation = DtlWhereConditionOperation.STARTSWITH;
                clause.value = "%" + clause.value;
                break;
            case 'endswith':
                clause.operation = DtlWhereConditionOperation.ENDSWITH;
                clause.value = clause.value + "%";
                break;
            case 'substring':
                clause.operation = DtlWhereConditionOperation.CONTAINS;
                clause.value = "%" + clause.value + "%";
                break;
            default:
                clause.operation = DtlWhereConditionOperation.EQ;
                break;
        }
        return clause;
    }

}