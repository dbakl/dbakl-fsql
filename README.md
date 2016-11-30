# dbakl-fsql
Fluent SQL Interface


## Usage

```
fsql()->insert()->into("SomeTable")->set(["Col1"=>"val1", "Col2"=>"val2"])
```

### Use Join-Map

```
fsql()->select()->from("SomeTable")->leftJoin("OtherTable")
```