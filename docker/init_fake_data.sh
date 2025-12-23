#!/bin/bash
#
# init_fake_data.sh
# 用于在每次 db 容器启动后，自动导入预置的测试数据
#
# 功能：
# 1. 等待 PostgreSQL 可连接
# 2. 检测 theta_ai schema 下的 3 个表是否存在
# 3. 如果表不存在，等待 60 秒后重试（最多 3 次）
# 4. 表存在后执行 SQL 导入（ON CONFLICT DO NOTHING 保证幂等）
#

set -e

# 配置
MAX_RETRIES=3
RETRY_INTERVAL=60
SQL_FILE="/sql/mirobody_fake_people.sql"

# 需要检测的表
REQUIRED_TABLES=("health_app_user" "health_user_profile_by_system" "th_series_data")
SCHEMA="theta_ai"

# 日志函数
log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo "[WARNING] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 等待 PostgreSQL 可连接
wait_for_postgres() {
    log_info "等待 PostgreSQL 可连接..."
    local max_wait=30
    local count=0
    
    while [ $count -lt $max_wait ]; do
        if pg_isready -h "$PGHOST" -p "${PGPORT:-5432}" -U "$PGUSER" > /dev/null 2>&1; then
            log_info "PostgreSQL 已就绪"
            return 0
        fi
        count=$((count + 1))
        sleep 1
    done
    
    log_error "等待 PostgreSQL 超时（${max_wait}秒）"
    return 1
}

# 检测单个表是否存在
check_table_exists() {
    local table_name=$1
    local result
    
    result=$(psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "$PGUSER" -d "$PGDATABASE" -tAc \
        "SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '$SCHEMA' 
            AND table_name = '$table_name'
        );")
    
    [ "$result" = "t" ]
}

# 检测所有必需的表是否存在
check_all_tables_exist() {
    local missing_tables=()
    
    for table in "${REQUIRED_TABLES[@]}"; do
        if ! check_table_exists "$table"; then
            missing_tables+=("$table")
        fi
    done
    
    if [ ${#missing_tables[@]} -eq 0 ]; then
        log_info "所有必需的表都已存在: ${REQUIRED_TABLES[*]}"
        return 0
    else
        log_info "缺少以下表: ${missing_tables[*]}"
        return 1
    fi
}

# 执行 SQL 导入
execute_sql_import() {
    log_info "开始执行 SQL 数据导入: $SQL_FILE"
    
    if [ ! -f "$SQL_FILE" ]; then
        log_error "SQL 文件不存在: $SQL_FILE"
        return 1
    fi
    
    # 执行 SQL 文件
    if psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_FILE" > /dev/null 2>&1; then
        log_info "SQL 数据导入成功完成"
        return 0
    else
        log_error "SQL 数据导入失败"
        return 1
    fi
}

# 主函数
main() {
    log_info "========== 开始初始化假人数据 =========="
    log_info "数据库: $PGHOST:${PGPORT:-5432}/$PGDATABASE"
    
    # 等待 PostgreSQL 就绪
    if ! wait_for_postgres; then
        log_error "无法连接到 PostgreSQL，退出"
        exit 1
    fi
    
    # 带重试的表检测
    local retry_count=0
    while [ $retry_count -lt $MAX_RETRIES ]; do
        retry_count=$((retry_count + 1))
        log_info "第 $retry_count 次尝试检测表（共 $MAX_RETRIES 次）"
        
        if check_all_tables_exist; then
            # 表存在，执行导入
            if execute_sql_import; then
                log_info "========== 初始化完成 =========="
                exit 0
            else
                log_error "SQL 导入失败，退出"
                exit 1
            fi
        fi
        
        # 表不存在，等待后重试
        if [ $retry_count -lt $MAX_RETRIES ]; then
            log_info "等待 ${RETRY_INTERVAL} 秒后重试..."
            sleep $RETRY_INTERVAL
        fi
    done
    
    # 所有重试都失败
    log_warning "经过 $MAX_RETRIES 次尝试后，theta_ai schema 下的表仍未就绪"
    log_warning "请确保应用程序已正确创建数据库表结构"
    log_info "========== 初始化未完成（表不存在） =========="
    exit 0  # 以 0 退出，避免容器重启循环
}

# 执行主函数
main

