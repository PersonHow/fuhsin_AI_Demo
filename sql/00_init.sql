CREATE DATABASE IF NOT EXISTS fuhsin_erp_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE fuhsin_erp_demo;

DROP TABLE IF EXISTS product_master_a;
CREATE TABLE product_master_a (
  product_id        VARCHAR(32) PRIMARY KEY,
  product_name      VARCHAR(255),
  product_model     VARCHAR(255),
  category          VARCHAR(255),
  price             DECIMAL(12,2),
  stock_qty         INT,
  manufacture_date  DATE,
  supplier          VARCHAR(255),
  status            VARCHAR(32),
  last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS product_warehouse_b;
CREATE TABLE product_warehouse_b (
  product_id          VARCHAR(32),
  product_name        VARCHAR(255),
  warehouse_location  VARCHAR(255),
  quantity            INT,
  last_inventory_date DATE,
  manager             VARCHAR(255),
  special_notes       VARCHAR(500),
  min_stock_level     INT,
  last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY (product_id)
);

DROP TABLE IF EXISTS customer_complaint_c;
CREATE TABLE customer_complaint_c (
  complaint_id     VARCHAR(32) PRIMARY KEY,
  complaint_date   DATE,
  customer_name    VARCHAR(255),
  customer_company VARCHAR(255),
  complaint_type   VARCHAR(255),
  severity         VARCHAR(32),
  description      TEXT,
  handler          VARCHAR(255),
  status           VARCHAR(32),
  resolution_date  DATE,
  last_modified    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ============================================================================
-- 主表：technical_documents (簡化版)
-- 用途：存放 PDF 原始資料和基本資訊
-- ============================================================================
CREATE TABLE IF NOT EXISTS technical_documents (
    -- 主鍵
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主鍵',
    doc_id VARCHAR(32) UNIQUE NOT NULL COMMENT '文檔唯一識別碼 (MD5 hash)',
    
    -- 文檔基本資訊
    doc_type VARCHAR(50) COMMENT '文檔類型 (ECN_NOTICE/ECN_APPLICATION/COMPLAINT/OTHER)',
    file_name VARCHAR(255) NOT NULL COMMENT '原始檔案名稱',
    file_size INT COMMENT '檔案大小 (bytes)',
    page_count INT COMMENT '頁數',
    
    -- 全文內容 (供全文檢索)
    content LONGTEXT COMMENT '文檔全文內容',
    
    -- 時間戳記
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
    
    -- 索引
    INDEX idx_doc_type (doc_type),
    INDEX idx_created_at (created_at),
    FULLTEXT idx_content (content)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='技術文件主表 - 存放PDF原始資料';

-- ============================================================================
-- 專屬表1：ecn_notices (設變通知單)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecn_notices (
    -- 主鍵
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_id VARCHAR(32) NOT NULL UNIQUE COMMENT '關聯 technical_documents.doc_id',
    
    -- 設變通知單特定欄位
    notice_number VARCHAR(100) COMMENT '通知單單號',
    application_number VARCHAR(100) COMMENT '設變申請單號',
    applicable_scope TEXT COMMENT '適用範圍',
    product_code VARCHAR(100) COMMENT '品號',
    product_name VARCHAR(255) COMMENT '品名',
    change_description TEXT COMMENT '設變說明',
    inventory_handling TEXT COMMENT '庫存處理',
    
    -- 庫存處理表資訊
    inventory_doc_number VARCHAR(100) COMMENT '庫存處理表-單號',
    inventory_product_name VARCHAR(255) COMMENT '庫存處理表-品名',
    inventory_notes TEXT COMMENT '庫存處理表-其他說明',
    
    -- 其他資訊
    ecn_date DATE COMMENT '設變單日期',
    applicant VARCHAR(100) COMMENT '申請人',
    before_change TEXT COMMENT '設變前',
    after_change TEXT COMMENT '設變後',
    
    -- 時間戳記
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 索引
    INDEX idx_notice_number (notice_number),
    INDEX idx_product_code (product_code),
    INDEX idx_ecn_date (ecn_date),
    FOREIGN KEY (doc_id) REFERENCES technical_documents(doc_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='設變通知單專屬表';

-- ============================================================================
-- 專屬表2：ecn_applications (設變申請單)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecn_applications (
    -- 主鍵
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_id VARCHAR(32) NOT NULL UNIQUE COMMENT '關聯 technical_documents.doc_id',
    
    -- 設變申請單特定欄位
    application_number VARCHAR(100) COMMENT '申請單號',
    ecn_date DATE COMMENT '設變單日期',
    product_name VARCHAR(255) COMMENT '品名',
    product_code VARCHAR(100) COMMENT '品號',
    reason TEXT COMMENT '緣由',
    
    -- 設變項目
    change_items TEXT COMMENT '設變項目',
    change_before TEXT COMMENT '設變項目-設變前',
    change_after TEXT COMMENT '設變項目-設變後',
    change_item_description TEXT COMMENT '設變項目-說明',
    
    -- 庫存處理
    inventory_handling TEXT COMMENT '庫存處理',
    inventory_product_code VARCHAR(100) COMMENT '庫存處理表-品號',
    inventory_quantity VARCHAR(50) COMMENT '庫存處理表-數量',
    inventory_notes TEXT COMMENT '庫存處理表-其他說明',
    
    -- 審查資訊
    execution_plan TEXT COMMENT '設變執行',
    meeting_suggestions TEXT COMMENT '研發單位審查意見-會議建議',
    review_notes TEXT COMMENT '研發單位審查意見-說明',
    review_decision TEXT COMMENT '審查核決',
    review_meeting_date DATE COMMENT '審查會議日期',
    
    -- 時間戳記
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 索引
    INDEX idx_application_number (application_number),
    INDEX idx_product_code (product_code),
    INDEX idx_ecn_date (ecn_date),
    FOREIGN KEY (doc_id) REFERENCES technical_documents(doc_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='設變申請單專屬表';

-- ============================================================================
-- 專屬表3：complaint_records (客訴資料)
-- ============================================================================
CREATE TABLE IF NOT EXISTS complaint_records (
    -- 主鍵
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_id VARCHAR(32) NOT NULL UNIQUE COMMENT '關聯 technical_documents.doc_id',
    
    -- 客訴特定欄位
    complaint_number VARCHAR(100) COMMENT '異常單號',
    complaint_type VARCHAR(100) COMMENT '開單類別',
    source_number VARCHAR(100) COMMENT '來源單號',
    production_notice_number VARCHAR(100) COMMENT '生產通知單號',
    
    -- 客戶與產品資訊
    customer_code VARCHAR(100) COMMENT '客戶代號',
    customer_name VARCHAR(255) COMMENT '客戶名稱',
    product_item VARCHAR(255) COMMENT '產品項目',
    product_code VARCHAR(100) COMMENT '品號',
    product_name VARCHAR(255) COMMENT '品名',
    shipping_factory VARCHAR(100) COMMENT '出貨廠別',
    
    -- 抱怨內容
    complaint_description TEXT COMMENT '抱怨內容描述',
    responsible_sales VARCHAR(100) COMMENT '承辦業務',
    complaint_analysis TEXT COMMENT '抱怨內容分析',
    
    -- 時間戳記
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 索引
    INDEX idx_complaint_number (complaint_number),
    INDEX idx_customer_code (customer_code),
    INDEX idx_product_code (product_code),
    FOREIGN KEY (doc_id) REFERENCES technical_documents(doc_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='客訴資料專屬表';

-- ============================================================================
-- 專屬表4：fmea_records (FMEA 分析表) - 新增
-- ============================================================================
CREATE TABLE IF NOT EXISTS fmea_records (
    -- 主鍵
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_id VARCHAR(32) NOT NULL UNIQUE COMMENT '關聯 technical_documents.doc_id',
    
    -- FMEA 基本資訊
    case_number VARCHAR(100) COMMENT '開發案號/案號 (如 R23091)',
    case_name VARCHAR(500) COMMENT '案件名稱',
    analysis_type VARCHAR(50) COMMENT '分析類型 (DFMEA/PFMEA)',
    product_type VARCHAR(100) COMMENT '產品別 (如 G鎖, L鎖)',
    
    -- 負責人資訊
    responsible_person VARCHAR(100) COMMENT '專案負責人/承辦人',
    analyst VARCHAR(255) COMMENT '分析人員',
    department VARCHAR(100) COMMENT '部門',
    
    -- FMEA 分析內容
    analysis_item VARCHAR(500) COMMENT '分析項目',
    failure_mode TEXT COMMENT '失效模式/問題描述',
    failure_effect TEXT COMMENT '失效影響/效應分析',
    failure_cause TEXT COMMENT '失效成因分析',
    
    -- 風險評分
    severity_s INT COMMENT '嚴重度 (S)',
    occurrence_o INT COMMENT '發生度 (O)',
    detection_d INT COMMENT '難檢度 (D)',
    rpn INT COMMENT '風險優先數 (RPN = S × O × D)',
    
    -- 改善措施
    current_control TEXT COMMENT '現行管控及檢測方式',
    corrective_action TEXT COMMENT '對策方案/改善措施',
    improvement_result TEXT COMMENT '改善結果',
    target_completion_date DATE COMMENT '預定完成日',
    execution_unit VARCHAR(255) COMMENT '執行單位',
    
    -- 改善後評分
    improved_severity INT COMMENT '改善後嚴重度',
    improved_occurrence INT COMMENT '改善後發生度',
    improved_detection INT COMMENT '改善後難檢度',
    improved_rpn INT COMMENT '改善後 RPN',
    
    -- 特殊標記
    is_customer_complaint BOOLEAN DEFAULT FALSE COMMENT '是否為客訴案',
    
    -- 審核資訊
    department_head VARCHAR(100) COMMENT '部主管',
    section_head VARCHAR(100) COMMENT '課主管',
    form_date DATE COMMENT '制訂日期',
    revision_date DATE COMMENT '修訂日期',
    version VARCHAR(50) COMMENT '版次',
    
    -- 時間戳記
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 索引
    INDEX idx_case_number (case_number),
    INDEX idx_product_type (product_type),
    INDEX idx_analysis_type (analysis_type),
    INDEX idx_rpn (rpn),
    INDEX idx_is_customer_complaint (is_customer_complaint),
    FOREIGN KEY (doc_id) REFERENCES technical_documents(doc_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='FMEA 分析表';

-- ============================================================================
-- 通用表：structured_documents (保留作為摘要索引)
-- ============================================================================
CREATE TABLE IF NOT EXISTS structured_documents (
    id INT AUTO_INCREMENT PRIMARY KEY,
    original_doc_id VARCHAR(50) NOT NULL UNIQUE COMMENT '關聯 technical_documents.doc_id',
    doc_type VARCHAR(50) COMMENT '文檔類型',
    doc_number VARCHAR(100) COMMENT '文檔編號',
    doc_date DATE COMMENT '文檔日期',
    
    -- 檔案資訊
    file_name VARCHAR(255),
    file_url TEXT,
    file_path TEXT,
    file_size BIGINT,
    file_hash VARCHAR(64),
    
    -- 產品資訊
    product_category VARCHAR(50),
    product_codes JSON,
    product_names JSON,
    related_doc_numbers JSON,
    
    -- 人員與部門
    applicant VARCHAR(100),
    department VARCHAR(100),
    responsible_units JSON,
    
    -- 摘要與關鍵字
    summary TEXT,
    keywords JSON,
    status VARCHAR(50),
    priority VARCHAR(20),
    
    -- 時間戳記
    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 索引
    INDEX idx_doc_number (doc_number),
    INDEX idx_doc_type (doc_type),
    INDEX idx_last_modified (last_modified),
    FOREIGN KEY (original_doc_id) REFERENCES technical_documents(doc_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='結構化文件摘要表';

-- ============================================================================
-- 處理日誌表 (保持不變)
-- ============================================================================
CREATE TABLE IF NOT EXISTS pdf_processing_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(32),
    status ENUM('processing', 'success', 'error', 'skipped') NOT NULL,
    error_message TEXT,
    process_time_ms INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='PDF處理日誌表';
