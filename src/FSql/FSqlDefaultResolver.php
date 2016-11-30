<?php
    /**
     * Created by PhpStorm.
     * User: matthes
     * Date: 30.11.16
     * Time: 12:13
     */

    namespace DbAkl\FSql;


    class FSqlDefaultResolver implements FSqlResolverInterface
    {

        public function resolveTableName(string $tableOrClassName) : string
        {
            if (strpos($tableOrClassName, "\\") !== false) {
                return basename($tableOrClassName);
            }
            return $tableOrClassName;
        }
    }