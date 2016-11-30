<?php
    /**
     * Created by PhpStorm.
     * User: matthes
     * Date: 30.11.16
     * Time: 12:09
     */

    namespace DbAkl\FSql;


    interface FSqlResolverInterface
    {


        /**
         * Resolve Class or TableName to the real Table Name
         *
         * Input:
         *
         * \Somme\Class => Class
         * TableName => TableName
         *
         * @param string $tableOrClassName
         * @return string
         */
        public function resolveTableName (string $tableOrClassName) : string;








    }