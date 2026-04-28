import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
import os

# 连接到 DuckDB 数据库（如果文件不存在则会自动创建）
ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "Data_Center" / "Data.db"
conn = duckdb.connect(str(DEFAULT_DB_PATH))

# ==========================================
# 创建数据表
# ==========================================

# 表1：新闻影响关联表 (news_impact)
conn.execute("""
    CREATE TABLE IF NOT EXISTS news_impact (
        News_ID VARCHAR,
        Ticker VARCHAR,
        Impact INTEGER CHECK (Impact IN (1, 0, -1))
    )
""")

# 表2：新闻主表 (news_master)
conn.execute("""
    CREATE TABLE IF NOT EXISTS news_master (
        News_ID VARCHAR PRIMARY KEY,
        Timestamp TIMESTAMP,
        Summary TEXT,
        Full_Text TEXT
    )
""")

# 表3：量价基础表 (price_volume)
conn.execute("""
    CREATE TABLE IF NOT EXISTS price_volume (
        Timestamp TIMESTAMP,
        Ticker VARCHAR,
        Interval VARCHAR,
        Open DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Close DOUBLE,
        Volume BIGINT
    )
""")

# 表4：动态因子数值表 (factor_values)
conn.execute("""
    CREATE TABLE IF NOT EXISTS factor_values (
        Timestamp TIMESTAMP,
        Ticker VARCHAR,
        Interval VARCHAR,
        Factor_Name VARCHAR,
        Factor_Value DOUBLE
    )
""")

# 表5：因子元数据主表 (factor_master)
conn.execute("""
    CREATE TABLE IF NOT EXISTS factor_master (
        Factor_Name VARCHAR PRIMARY KEY,
        Factor_Formula TEXT,
        Generation_Time TIMESTAMP,
        Historical_Sharpe DOUBLE
    )
""")

print("五个数据表创建成功！")

class QuantDatabase:
    """量化数据库操作封装类"""
    
    def __init__(self, db_path: str = str(DEFAULT_DB_PATH)):
        # 确保目录存在，防止因为没有 Data_Center 文件夹而报错
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.conn = duckdb.connect(db_path)
        self._create_tables_if_not_exist()
        print(f"数据库已连接，路径: {db_path}，表结构已就绪！")

    def _create_tables_if_not_exist(self):
        """内部方法：确保所有必备数据表存在"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS news_impact (
                News_ID VARCHAR, Ticker VARCHAR, Impact INTEGER CHECK (Impact IN (1, 0, -1))
            );
            CREATE TABLE IF NOT EXISTS news_master (
                News_ID VARCHAR PRIMARY KEY, Timestamp TIMESTAMP, Summary TEXT, Full_Text TEXT
            );
            CREATE TABLE IF NOT EXISTS price_volume (
                Timestamp TIMESTAMP, Ticker VARCHAR, Interval VARCHAR, Open DOUBLE, High DOUBLE, 
                Low DOUBLE, Close DOUBLE, Volume BIGINT
            );
            CREATE TABLE IF NOT EXISTS factor_values (
                Timestamp TIMESTAMP, Ticker VARCHAR, Interval VARCHAR, Factor_Name VARCHAR, Factor_Value DOUBLE
            );
            CREATE TABLE IF NOT EXISTS factor_master (
                Factor_Name VARCHAR PRIMARY KEY, Factor_Formula TEXT, 
                Generation_Time TIMESTAMP, Historical_Sharpe DOUBLE
            );
        """)
        self._ensure_column("price_volume", "Interval", "VARCHAR", "'unknown'")
        self._ensure_column("factor_values", "Interval", "VARCHAR", "'unknown'")

    def _ensure_column(self, table_name: str, column_name: str, column_type: str, default_sql: str):
        """兼容旧版 Data.db：缺列时自动补齐。"""
        columns = {row[0] for row in self.conn.execute(f"DESCRIBE {table_name}").fetchall()}
        if column_name not in columns:
            self.conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type} DEFAULT {default_sql}"
            )

    # ==========================================
    # 增 (Create / Insert)
    # ==========================================
    
    def insert_new_factor(self, factor_name: str, formula: str, gen_time, sharpe: float):
        """向 factor_master 注册由 AI 发明的新因子"""
        self.conn.execute("""
            INSERT INTO factor_master (Factor_Name, Factor_Formula, Generation_Time, Historical_Sharpe)
            VALUES (?, ?, ?, ?)
        """, (factor_name, formula, gen_time, sharpe))

    def insert_news_impact_batch(self, impact_records: List[Tuple[str, str, int]]):
        """批量插入新闻影响关联表，格式: [(News_ID, Ticker, Impact), ...]"""
        self.conn.executemany("""
            INSERT INTO news_impact (News_ID, Ticker, Impact)
            VALUES (?, ?, ?)
        """, impact_records)

    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        通用极速插入法：将 Pandas DataFrame 直接写入指定表
        """
        # 只要 df 在当前作用域，DuckDB 就能自动识别
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")

    # ==========================================
    # 查 (Read / Select)
    # ==========================================
    
    def fetch_query_as_df(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """执行任意查询，并直接返回 Pandas DataFrame"""
        if params:
            return self.conn.execute(query, params).df()
        return self.conn.execute(query).df()

    def get_factor_metadata(self, min_sharpe: float = 0.0) -> pd.DataFrame:
        """获取夏普比率大于指定阈值的因子元数据"""
        return self.fetch_query_as_df("""
            SELECT * FROM factor_master 
            WHERE Historical_Sharpe >= ? 
            ORDER BY Historical_Sharpe DESC
        """, (min_sharpe,))

    # ==========================================
    # 改 (Update)
    # ==========================================
    
    def update_factor_sharpe(self, factor_name: str, new_sharpe: float):
        """根据模拟盘最新回测结果，更新因子的历史夏普比率"""
        self.conn.execute("""
            UPDATE factor_master 
            SET Historical_Sharpe = ?
            WHERE Factor_Name = ?
        """, (new_sharpe, factor_name))

    # ==========================================
    # 删 (Delete)
    # ==========================================
    
    def delete_underperforming_factors(self, min_sharpe_threshold: float):
        """清理表现不达标的劣质因子及其相关数据"""
        self.conn.execute("DELETE FROM factor_master WHERE Historical_Sharpe < ?", (min_sharpe_threshold,))

    def __del__(self):
        """析构函数，确保对象被销毁时自动关闭数据库连接"""
        try:
            self.conn.close()
        except Exception:
            pass
