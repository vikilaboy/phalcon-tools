<?php

namespace Vikilaboy\Phalcon\Paginator\Adapter;

use Phalcon\Mvc\Model\Query\Builder;
use Phalcon\Paginator\Exception;
use Phalcon\Paginator\Adapter\QueryBuilder as QueryBuilderP;

class QueryBuilder extends QueryBuilderP
{

    public function getPaginate()
    {
        $originalBuilder = $this->builder;
        $columns = $this->columns;

        /**
         * We make a copy of the original builder to leave it as it is
         */
        /** @var Builder $builder */
        $builder = clone $originalBuilder;

        /**
         * We make a copy of the original builder to count the total of records
         */
        $totalBuilder = clone $builder;

        $limit = $this->limitRows;
        $numberPage = (int) $this->page;

        if (!$numberPage) {
            $numberPage = 1;
        }

        $number = $limit * ($numberPage - 1);

        /**
         * Set the limit clause avoiding negative offsets
         */
        if ($number < $limit) {
            $builder->limit($limit);
        } else {
            $builder->limit($limit, $number);
        }

        $query = $builder->getQuery();

        if ($numberPage == 1) {
            $before = 1;
        } else {
            $before = $numberPage - 1;
        }

        /**
         * Execute the query an return the requested slice of data
         */
        $items = $query->execute();

        $hasHaving = !empty($totalBuilder->getHaving());

        $groups = $totalBuilder->getGroupBy();

        $hasGroup = !(empty($groups));

        /**
         * Change the queried columns by a COUNT(*)
         */

        if ($hasHaving && !$hasGroup) {
            if (empty($columns)) {
                throw new Exception("When having is set there should be columns option provided for which calculate row count");
            }
            $totalBuilder->columns($columns);
        } else {
            $totalBuilder->columns("COUNT(*) [rowcount]");
        }

        /**
         * Change 'COUNT()' parameters, when the query contains 'GROUP BY'
         */
        if ($hasGroup) {

            if (is_array($groups)) {
                $groupColumn = implode(", ", $groups);
            } else {
                $groupColumn = $groups;
            }

            if (!$hasHaving) {
                $totalBuilder->groupBy(null)->columns(["COUNT(DISTINCT ".$groupColumn.") AS [rowcount]"]);
            } else {
                $cols = ["DISTINCT " . $groupColumn];
                if (!empty($columns)) {
                    $cols[] = $columns;
                }

                $totalBuilder->columns($cols);
            }
        }

        /**
         * Remove the 'ORDER BY' clause, PostgreSQL requires this
         */
        $totalBuilder->orderBy(null);

        /**
         * Obtain the PHQL for the total query
         */
        $totalQuery = $totalBuilder->getQuery();

        /**
         * Obtain the result of the total query
         * If we have having perform native count on temp table
         */
        if ($hasHaving) {
            $sql = $totalQuery->getSql();

            /** @var \Phalcon\Mvc\Model $modelClass */
            $modelClass = $builder->getFrom();

            if (is_array($modelClass)) {
                $modelClass = array_values($modelClass)[0];
            }

            /** @var \Phalcon\Mvc\Model $model */
            $model = new $modelClass();
            $dbService = $model->getReadConnectionService();
            $db = $totalBuilder->getDI()->get($dbService);
            $row = $db->fetchOne("SELECT COUNT(*) as \"rowcount\" FROM (" .  $sql["sql"] . ") AS T1", \Phalcon\Db\Enum::FETCH_ASSOC, $sql["bind"]);
            $rowCount = $row ? intval($row["rowcount"]) : 0;
            $totalPages = intval(ceil($rowCount / $limit));
        } else {
            $result = $totalQuery->execute();
            $row = $result->getFirst();
            $rowCount = $row ? intval($row->rowcount) : 0;
            $totalPages = intval(ceil($rowCount / $limit));
        }

        if ($numberPage < $totalPages) {
            $next = $numberPage + 1;
        } else {
            $next = $totalPages;
        }

        $page = new \stdClass();
        $page->items = $items;
        $page->first = 1;
        $page->before = $before;
        $page->current = $numberPage;
        $page->last = $totalPages;
        $page->next = $next;
        $page->total_pages = $totalPages;
        $page->total_items = $rowCount;
        $page->limit = $this->limitRows;

        return $page;
    }
}