from pathlib import Path

import pytest

from flink_skill_common.agents.sources import build_source_ddl_agent, generate_source_ddls
from flink_skill_common.config import HarnessContext, configure
from flink_skill_common.convergence import clean_flink_sql_and_validate
from flink_skill_common.response_io import extract_sql_blocks

__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
_HARNESS = HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT)
configure(_HARNESS)

def test_extract_sql_blocks():
    response = """
    
    """
    ddls, dmls = extract_sql_blocks(response)
    assert len(ddls) == 0
    assert len(dmls) == 0
    resp ="""
    ```sql
DDL:
CREATE TABLE IF NOT EXISTS george_martin (
    -- columns inferred from SELECT * on all_publications
    -- define explicit column types once all_publications schema is available
);
```

```sql
DML:
INSERT INTO george_martin SELECT * FROM all_publications WHERE author = 'George R. R. Martin';
```
"""
    ddls, dmls = extract_sql_blocks(resp)
    assert len(ddls) == 1
    assert len(dmls) == 1
    print(f"DDLs: {ddls}")
    print(f"DMLs: {dmls}")


def test_source_agent():
    response = """
    

```sql
DDL:
CREATE TABLE IF NOT EXISTS george_martin (
    -- columns inferred from SELECT * on all_publications
    -- define explicit column types once all_publications schema is available
);
```

```sql
DML:
INSERT INTO george_martin SELECT * FROM all_publications WHERE author = 'George R. R. Martin';
```
"""
    clean_flink_sql_and_validate(response, 
    "george_martin", 
    "CREATE TABLE all_publications (id INT, title STRING, author STRING);", False, Path("test_output"),)
