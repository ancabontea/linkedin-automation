/**
 * LINKEDIN ANALYTICS AUTOMATION SYSTEM - COMPLETE VERSION
 * Handles 6+ LinkedIn data sources with enhanced error handling and performance
 * Author: Optimized for Google Apps Script
 * Version: 2.1 - Complete Implementation
 */

// ===================================================================
// CONFIGURATION AND CONSTANTS
// ===================================================================

const CONFIG = {
  // Processing Settings
  CURRENT_YEAR: 2025,
  USE_AUTO_DATE_DETECTION: true,
  BATCH_SIZE: 100,
  MAX_PROCESSING_TIME_MS: 270000, // 4.5 minutes (safe margin)
  MEMORY_CLEANUP_INTERVAL: 10,
  MAX_FILES_PER_BATCH: 50, // Prevent memory overflow
  
  // Folder Configuration
  INCOMING_FOLDER_ID: '176FVQ5flxmZJ6ijNqZiLSRO3RyV0mLB8',
  PROCESSED_FOLDER_ID: '1WZHbQEF0RHEItVfLz7_QvLlj6IyjjLg6',
  
  // Sheet IDs - Campaign & Demographics  
  CAMPAIGN_PERFORMANCE_SHEET_ID: '1Ps4OtatyFXdEvzePdCKx46LxGAWv27eBcIyLpSJYcmc',
  DEMOGRAPHICS_OVERVIEW_SHEET_ID: '1hycpJam8sqSsj6RpC5s_eJqc_kNJqW1IWjlHuPIlxXc',
  DEMOGRAPHICS_COMPANY_SHEET_ID: '1WTiSCWBC-yfnewset8YmrCyXd-qadi_9kwII3GAwdnI',
  DEMOGRAPHICS_INDUSTRY_SHEET_ID: '1LN5XlQqWzX5la6k4Pn6oaDqOO-h5v3uD-JFjOUh2H3k',
  DEMOGRAPHICS_SIZE_SHEET_ID: '1eQz_NAoBepGwj0XabsBaTUjg1-jcNPJp47DAP3wvruI',
  DEMOGRAPHICS_LOCATION_SHEET_ID: '11Fq6OHFbpoqqa6Pu0uzak_lsK2iymDtINjG0yANHzo8',
  DEMOGRAPHICS_SENIORITY_SHEET_ID: '1YYNdeGghUrxNjeP5XTpn1htPsLVb3Wel04Ne3AddPIE',
  DEMOGRAPHICS_FUNCTION_SHEET_ID: '16OhHe5XxX7bywX3pjGGOj0iaNXrFHbQWTH4FwyST6Rg',
  DEMOGRAPHICS_JOB_TITLES_SHEET_ID: '1wYc-J7a6jcIwu540JbyapcxPYGBYfwNy_CKKWUkKeUE',

  // Content Analytics
  CONTENT_PERFORMANCE_SHEET_ID: '1LrDbE4vyXzBK-MS9AZfHMXA_TNCoJ6Ox2cC0i_a5zO0',
  CONTENT_ANALYSIS_SHEET_ID: '1IS9ZJ7sR0PMRnxyPWQjMc8XDIfUU1bpk6UcufEJArZs',
  
  // Visitor Analytics
  VISITOR_METRICS_SHEET_ID: '1d48DHVfHwTtJlJsfJ1afXknpy-2cRqnlauLV3SaB9s8',
  VISITOR_COMPANY_SIZE_SHEET_ID: '1AcauyhsK9UuwlbVWFgdEglTIV8iqPuF9ahUckEJJ9Jo',
  VISITOR_INDUSTRY_SHEET_ID: '1xmHsY_dG8XrP9zjkm4tfQH8de61vr3eOeG5ZcMVOcX8',
  VISITOR_SENIORITY_SHEET_ID: '1uyLTAmxcLdHLCpJaVgrgqt3uLSFdtEZbBdKtVO_0aNc',
  VISITOR_FUNCTION_SHEET_ID: '1fd9dl9s-GhZUMMXSub_d51ju1LHTWhgg8genc7dM0tg',
  VISITOR_LOCATION_SHEET_ID: '1y2yT3z6gzQASNTzZ4XK2UKZl2inMmb23D2UjLkiJSRo',
  
  // Follower Analytics
  FOLLOWER_METRICS_SHEET_ID: '10-celg7Imy2bGlePZziBcM81rHUGxitPpvMyJ0JB34E',
  FOLLOWER_COMPANY_SIZE_SHEET_ID: '1tBFy1wrEsWinc7SrJeWBmYKP5HY0Seg1oHU98lWYbVI',
  FOLLOWER_INDUSTRY_SHEET_ID: '1PWQt0YvWtZURVzAUpfaAroggWoXy3LvmB1ksQmSiLZ8',
  FOLLOWER_SENIORITY_SHEET_ID: '15WVVIbMOowmkxLuIpU0qL2jRuoXvsvuH1r550BbGoM0',
  FOLLOWER_FUNCTION_SHEET_ID: '14YR5QPWZGTj3nzjPYyrM39uQIHYnqACeWdKdyHUrT6Q',
  FOLLOWER_LOCATION_SHEET_ID: '13BO6QHjl9jJA_xIwT8Niup3Aa3IGj0lEHuJUSv_neLs',
  
  // Competitor Analytics
  COMPETITOR_PERFORMANCE_SHEET_ID: '1h_LeyoNBHDNbQBAohEBnzL_ATVz9d_VQRAX6uOLUOT8',
  COMPETITOR_ANALYSIS_SHEET_ID: '1aWc6iC4HvWXBzqpIjEQ7GtRJ0Jk2hCtbbFvBfOAHSCc',
  
  // Master Dashboard
  MASTER_DASHBOARD_SHEET_ID: '158Jo-8mcEnkFTLZ4vJowST5LabagR6DHcjf7Q5rO8JY',
  DEFAULT_SHEET_NAME: 'Sheet1' // More explicit naming
};

// Processing constants
const CONSTANTS = {
  MAX_HEADER_ROWS: 5,
  MIN_DATA_ROWS: 1,
  MIN_COLUMN_FILL_RATIO: 0.3,
  CSV_CHUNK_SIZE: 1000,
  RETRY_ATTEMPTS: 3,
  RETRY_DELAY_MS: 1000,
  MAX_FILE_SIZE_MB: 10
};

// Enhanced file detection patterns
const FILE_PATTERNS = {
  // Campaign files (LinkedIn Ads)
  campaign_performance: /campaign.*performance|ads.*performance|sponsored.*content/i,
  demographics_report: /demographics.*report|audience.*demographics|campaign.*demographics/i,
  
  // Organic content files
  content_analytics: /content.*analytics|post.*performance|organic.*content/i,
  
  // Visitor analytics files  
  visitor_metrics: /visitor.*metrics|page.*views|visitor.*overview/i,
  visitor_company_size: /company.*size.*table|visitors.*company.*size/i,
  visitor_industry: /industry.*table|visitors.*industry/i,
  visitor_seniority: /seniority.*table|visitors.*seniority/i,
  visitor_function: /job.*function.*table|visitors.*function/i,
  visitor_location: /location.*table|visitors.*location/i,
  
  // Follower files - Updated patterns to handle spaces and variations
  follower_metrics: /follower.*metrics|new.*followers|follower.*new.*followers/i,
  follower_company_size: /follower.*company.*size|followers.*company.*size/i,
  follower_industry: /follower.*industry|followers.*industry/i,
  follower_seniority: /follower.*seniority|followers.*seniority/i,
  follower_function: /follower.*function|followers.*function|follower.*job.*function/i,
  follower_location: /follower.*location|followers.*location/i,
  
  // Competitor files
  competitor_analytics: /competitor.*analytics|competitive.*analysis/i
};

// Global state management
let processingState = {
  data: new Map(),
  startTime: null,
  processedCount: 0,
  errorCount: 0,
  memoryUsage: 0
};


// ADD THIS NEW SECTION AFTER CONFIG
const ENHANCED_CONFIG = {
  ...CONFIG,
  
  // Monthly Comparison Sheets - for time-based analysis
  COMPARISON_SHEETS: {
    CAMPAIGN_TRENDS: '16wowDXkry8tU-xu8rdOr3KeN4vqgspGNlgNCbEGq_RU',
    DEMOGRAPHICS_TRENDS: '1MVrWwP5xtd5olG3YxmUeWl4mQcym3KV0O8DptSBX5YU',
    CONTENT_TRENDS: '1CiDLxdFeTs6KDzCOzHmg8_FD5b56D4bssOeSLUL868k',
    FOLLOWER_TRENDS: '1Uc4Wszp2r-WBHWmcvSfhitaP3CsyFZVflQg9hR_tP0w',
    VISITOR_TRENDS: '1244AdWrnS91FFZ_UwkoVmvrtvV6ZR_XNoidKx3leXnU',
    MONTHLY_COMPARISON_DASHBOARD: '10FdaZEdFoz5R71uIU4k9XfzXpT0Apg9SXt0oYhL35zA'
  },
  
  // Trend Analysis Settings
  TREND_SETTINGS: {
    RETAIN_MONTHS: 12,
    ARCHIVE_AFTER_MONTHS: 24,
    COMPARISON_PERIODS: ['current_month', 'previous_month', 'same_month_last_year']
  }
};
// ===================================================================
// UTILITY FUNCTIONS
// ===================================================================
function logWithTime(message, level = 'INFO') {
  const timestamp = new Date().toISOString();
  const elapsed = processingState.startTime ? 
    `+${((Date.now() - processingState.startTime) / 1000).toFixed(1)}s` : '';
  console.log(`[${timestamp}] [${level}] ${elapsed} ${message}`);
}

function getCurrentDateInfo() {
  const now = new Date();
  return {
    year: now.getFullYear(),
    week: getWeekNumber(now),
    dateString: formatDate(now),
    timestamp: now
  };
}

function getWeekNumber(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function formatDate(date) {
  if (!date) return '';
  
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  
  return `${year}-${month}-${day}`;
}
// Helper function to identify company size patterns
function isLikelyCompanySize(value) {
  if (!value || typeof value !== 'string') return false;
  
  const companyPatterns = [
    /^\d+-\d+$/,           // 2-10, 11-50, etc.
    /^\d+\+$/,             // 10001+, etc.
    /^\d+k\+$/i,           // 10k+, etc.
    /^1$/                  // Single employee companies
  ];
  
  return companyPatterns.some(pattern => pattern.test(value.trim()));
}

// Helper function to identify values that look like dates but aren't
function isLikelyNotDate(value) {
  if (!value || typeof value !== 'string') return false;
  
  const notDatePatterns = [
    /^\d+-\d+$/,           // Range values like 2-10
    /^\d+\+$/,             // Plus values like 10001+
    /^\d+k\+$/i,           // Values like 10k+
  ];
  
  return notDatePatterns.some(pattern => pattern.test(value.trim()));
}
// ===================================================================
// FILE VALIDATION AND DETECTION
// ===================================================================

function isLinkedInFile(fileName) {
  if (!fileName) return false;
  
  const patterns = [
    /linkedin/i,
    /campaign/i,
    /content/i,
    /visitor/i,
    /follower/i,
    /competitor/i,
    /demographics/i,
    /sponsored/i,
    /organic/i
  ];
  
  return patterns.some(pattern => pattern.test(fileName)) ||
         Object.values(FILE_PATTERNS).some(pattern => pattern.test(fileName));
}

function detectFileType(fileName) {
  if (!fileName) return 'unknown';
  
  // More specific patterns first to avoid conflicts
  for (const [type, pattern] of Object.entries(FILE_PATTERNS)) {
    if (pattern.test(fileName)) {
      logWithTime(`Detected file type: ${type} for ${fileName}`);
      return type;
    }
  }
  
  logWithTime(`Unknown file type for: ${fileName}`, 'WARN');
  return 'unknown';
}

function validateFile(file) {
  try {
    if (!file) return { valid: false, error: 'File is null' };
    
    const fileName = file.getName();
    if (!fileName) return { valid: false, error: 'No filename' };
    
    const size = file.getSize();
    if (size > CONSTANTS.MAX_FILE_SIZE_MB * 1024 * 1024) {
      return { valid: false, error: `File too large: ${(size / 1024 / 1024).toFixed(1)}MB` };
    }
    
    if (size < 100) {
      return { valid: false, error: 'File too small' };
    }
    
    // Check file extension
    const extension = fileName.toLowerCase().split('.').pop();
    if (!['csv', 'txt', 'tsv'].includes(extension)) {
      return { valid: false, error: `Invalid file type: ${extension}` };
    }
    
    return { valid: true };
    
  } catch (error) {
    return { valid: false, error: error.message };
  }
}


function extractDateFromFileName(fileName) {
  const dateInfo = getCurrentDateInfo();
  
  // Define month mapping first
  const monthMap = {
    'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
    'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6,
    'jul': 7, 'july': 7, 'aug': 8, 'august': 8, 'sep': 9, 'september': 9,
    'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12
  };
  
  const patterns = [
  // Full month names with year (remove word boundaries for more flexibility)
  /(january|february|march|april|may|june|july|august|september|october|november|december)(\d{4})/i,
  
  // Short month names with year  
  /(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})/i,
  
  // Day + Full month combinations 
  /(\d{1,2})(january|february|march|april|may|june|july|august|september|october|november|december)(\d{4})?/i,
  
  // Day + Short month combinations
  /(\d{1,2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})?/i,
  
  // ISO formats
  /(\d{4})-(\d{1,2})-(\d{1,2})/,
  /(\d{4})_(\d{1,2})_(\d{1,2})/,
  
  // Year_Month formats
  /(\d{4})_(\d{1,2})/,
  
  // Week formats
  /(\d{4})_week_?(\d{1,2})/i,
  /week_?(\d{1,2})_?(\d{4})/i
];

  for (const pattern of patterns) {
    const match = fileName.match(pattern);
    if (match) {
      let year, month, day;
      
      logWithTime(`Pattern matched: ${pattern.toString()}, Match: ${JSON.stringify(match)}`);
      
      // Handle full month name patterns (august2025, january2025)
      if (match[1] && monthMap[match[1].toLowerCase()]) {
        month = monthMap[match[1].toLowerCase()];
        year = parseInt(match[2]) || dateInfo.year;
        day = 1; // Default to first day of month
        
        logWithTime(`Full month pattern: ${match[1]} -> month ${month}, year ${year}`);
      }
      // Handle day+full month patterns (26august2025)
      else if (match[1] && !isNaN(match[1]) && match[2] && monthMap[match[2].toLowerCase()]) {
        day = parseInt(match[1]);
        month = monthMap[match[2].toLowerCase()];
        year = parseInt(match[3]) || dateInfo.year;
        
        logWithTime(`Day+month pattern: day ${day}, ${match[2]} -> month ${month}, year ${year}`);
      }
      // Handle short month patterns (aug2025, jul2025)
      else if (match[1] && match[1].length <= 3 && monthMap[match[1].toLowerCase()]) {
        month = monthMap[match[1].toLowerCase()];
        year = parseInt(match[2]) || dateInfo.year;
        day = 1;
        
        logWithTime(`Short month pattern: ${match[1]} -> month ${month}, year ${year}`);
      }
      // Handle day+short month patterns (26aug2025)  
      else if (match[1] && !isNaN(match[1]) && match[2] && match[2].length <= 3 && monthMap[match[2].toLowerCase()]) {
        day = parseInt(match[1]);
        month = monthMap[match[2].toLowerCase()];
        year = parseInt(match[3]) || dateInfo.year;
        
        logWithTime(`Day+short month pattern: day ${day}, ${match[2]} -> month ${month}, year ${year}`);
      }
      // Handle numeric patterns (2025-07-15, 2025_07, etc.)
      else if (match[1] && !isNaN(match[1])) {
        year = parseInt(match[1]);
        month = match[2] ? parseInt(match[2]) : null;
        day = match[3] ? parseInt(match[3]) : 1;
        
        // For week patterns, estimate month from week number
        if (fileName.toLowerCase().includes('week') && !month) {
          const weekNum = parseInt(match[2] || match[1]);
          month = Math.ceil(weekNum / 4.33); // Rough week to month conversion
        }
        
        logWithTime(`Numeric pattern: year ${year}, month ${month}, day ${day}`);
      }
      
      // Validate and return - FIXED VERSION
      if (year && month && month >= 1 && month <= 12) {
        // Create date using UTC to avoid timezone shifts
        const extractedDate = new Date(Date.UTC(year, month - 1, day || 1));
        
        logWithTime(`Successfully extracted: ${year}-${month}-${day || 1} from "${fileName}"`);
        logWithTime(`UTC Date created: ${extractedDate.toISOString()}`);
        
        return {
          year: year,
          month: month,
          day: day || 1,
          week: getWeekNumber(extractedDate),
          original: match[0],
          detected: true,
          extractedDate: extractedDate,
          dateString: `${year}-${String(month).padStart(2, '0')}-${String(day || 1).padStart(2, '0')}`
        };
      } else {
        logWithTime(`Invalid date extracted: year=${year}, month=${month}, day=${day}`, 'WARN');
      }
    }
  }
  
  // Default fallback
  return {
    ...dateInfo,
    detected: false,
    original: dateInfo.dateString,
    extractedDate: new Date() // Fallback to current date
  };
}

// FIXED: Extract period dates using the FILENAME date, not upload date
function extractPeriodDatesFromFilename(fileName) {
  const fileDate = extractDateFromFileName(fileName); // This extracts from filename
  
  // Use the extracted date from filename, not current date
  let year, month;
  
  if (fileDate.detected && fileDate.year && fileDate.month) {
    year = fileDate.year;
    month = fileDate.month;
  } else {
    // Fallback to current date only if no date found in filename
    const now = new Date();
    year = now.getFullYear();
    month = now.getMonth() + 1;
    logWithTime(`Warning: Could not extract date from filename "${fileName}", using current date`, 'WARN');
  }
  
  // Calculate period start and end dates using UTC to avoid timezone issues
  const periodStart = new Date(Date.UTC(year, month - 1, 1));
  const periodEnd = new Date(Date.UTC(year, month, 0));
  
  // Format dates as DD/MM/YYYY for display
  const formatDateDDMMYYYY = (date) => {
    const day = String(date.getUTCDate()).padStart(2, '0');
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const year = date.getUTCFullYear();
    return `${day}/${month}/${year}`;
  };
  
  return {
    ...fileDate,
    year: year,
    month: month,
    month_name: getMonthName(month),
    quarter: getQuarter(month),
    year_month: `${year}-${String(month).padStart(2, '0')}`,
    
    // Period boundary dates based on FILENAME date
    period_start_date: formatDateDDMMYYYY(periodStart),
    period_end_date: formatDateDDMMYYYY(periodEnd),
    
    // Additional period info
    days_in_period: periodEnd.getUTCDate(),
    period_description: `${getMonthName(month)} ${year}`,
    
    // Add the source of the date for debugging
    date_source: fileDate.detected ? 'filename' : 'fallback'
  };
}
// ===================================================================
// ENHANCED CSV PARSING
// ===================================================================
// Enhanced date extraction for better temporal tracking
function extractTemporalInfo(fileName) {
  const baseDate = extractDateFromFileName(fileName);
  const now = new Date();
  
  return {
    ...baseDate,
    month_name: getMonthName(baseDate.month || now.getMonth() + 1),
    quarter: getQuarter(baseDate.month || now.getMonth() + 1),
    year_month: `${baseDate.year || now.getFullYear()}-${String(baseDate.month || now.getMonth() + 1).padStart(2, '0')}`,
    period_key: `${baseDate.year || now.getFullYear()}_${String(baseDate.month || now.getMonth() + 1).padStart(2, '0')}`,
    is_current_month: isCurrentMonth(baseDate),
    months_ago: getMonthsAgo(baseDate)
  };
}

function getMonthName(monthNumber) {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return months[monthNumber - 1] || 'Unknown';
}

function getQuarter(monthNumber) {
  return Math.ceil(monthNumber / 3);
}

function isCurrentMonth(dateInfo) {
  const now = new Date();
  return (dateInfo.year || now.getFullYear()) === now.getFullYear() &&
         (dateInfo.month || now.getMonth() + 1) === now.getMonth() + 1;
}

function getMonthsAgo(dateInfo) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1;
  const fileYear = dateInfo.year || currentYear;
  const fileMonth = dateInfo.month || currentMonth;
  
  return (currentYear - fileYear) * 12 + (currentMonth - fileMonth);
}

// Optimized temporal sheet update - maintains historical data with monthly structure
function updateSheetTemporal(baseSheetId, sheetName, headers, data, reportType, temporalInfo, comparisonSheetId = null) {
  const results = { source_updated: false, comparison_updated: false };
  
  try {
    // 1. Update monthly-specific sheet (append mode with monthly sections)
    results.source_updated = updateMonthlySheet(
      baseSheetId, 
      sheetName, 
      headers, 
      data, 
      reportType, 
      temporalInfo
    );
    
    // 2. Update trends comparison sheet if provided
    if (comparisonSheetId) {
      results.comparison_updated = updateTrendsSheet(
        comparisonSheetId, 
        CONFIG.DEFAULT_SHEET_NAME, 
        headers, 
        data, 
        reportType, 
        temporalInfo
      );
    }
    
    return results.source_updated;
    
  } catch (error) {
    logWithTime(`Temporal sheet update failed: ${error.message}`, 'ERROR');
    return false;
  }
}

// PERIOD DATE ENHANCEMENT FOR EXISTING SHEETS
// Adds start/end period dates to your current detailed sheets for comparison

// FIXED: Extract period dates using the FILENAME date, not upload date
// Enhanced date extraction with better pattern matching
// Enhanced date extraction with better pattern matching
function extractPeriodDatesFromFilename(fileName) {
  const fileDate = extractDateFromFileName(fileName);
  
  let year, month;
  
  if (fileDate.detected && fileDate.year && fileDate.month) {
    year = fileDate.year;
    month = fileDate.month;
  } else {
    const now = new Date();
    year = now.getFullYear();
    month = now.getMonth() + 1;
    logWithTime(`Warning: Could not extract date from filename "${fileName}", using current date`, 'WARN');
  }
  
  const periodStart = new Date(Date.UTC(year, month - 1, 1));
  const periodEnd = new Date(Date.UTC(year, month, 0));
  
  // Format dates for Looker Studio (YYYY-MM-DD format)
  const formatLookerDate = (date) => {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };
  
  return {
    ...fileDate,
    year: year,
    month: month,
    month_name: getMonthName(month),
    quarter: getQuarter(month),
    year_month: `${year}-${String(month).padStart(2, '0')}`,
    
    // Looker Studio compatible date formats
    period_start_date: formatLookerDate(periodStart),  // 2025-08-01
    period_end_date: formatLookerDate(periodEnd),      // 2025-08-31
    
    days_in_period: periodEnd.getUTCDate(),
    period_description: `${getMonthName(month)} ${year}`,
    date_source: fileDate.detected ? 'filename' : 'fallback'
  };
}

// Update monthly sheet with temporal organization
function updateMonthlySheet(sheetId, sheetName, headers, data, reportType, temporalInfo) {
  try {
    const sheet = getOrCreateSheet(sheetId, sheetName);
    
    // Add temporal metadata to headers
    const temporalHeaders = [
      'Upload_Month',
      'Month_Name', 
      'Quarter',
      'Year_Month',
      'Months_Ago',
      'Upload_Date',
      'Upload_Time',
      ...headers
    ];
    
    // Check if this is first upload or new month
    const isFirstUpload = sheet.getLastRow() === 0;
    const needsNewMonthSection = !isFirstUpload && temporalInfo.is_current_month;
    
    let startRow = 1;
    
    if (isFirstUpload) {
      // First upload - create headers
      sheet.getRange(1, 1, 1, temporalHeaders.length).setValues([temporalHeaders]);
      formatTemporalHeaders(sheet, temporalHeaders.length);
      startRow = 2;
    } else {
      // Append mode - find insertion point
      startRow = sheet.getLastRow() + 1;
      
      // Add month separator if it's a new month
      if (needsNewMonthSection) {
        addMonthSeparator(sheet, startRow, temporalInfo, temporalHeaders.length);
        startRow += 2; // Account for separator and spacing
      }
    }
    
    // Prepare temporal data
    const uploadDate = formatDate(new Date());
    const uploadTime = new Date().toLocaleTimeString();
    
    const temporalData = data.map(row => {
      const temporalRow = [
        temporalInfo.period_key,
        temporalInfo.month_name,
        `Q${temporalInfo.quarter}`,
        temporalInfo.year_month,
        temporalInfo.months_ago,
        uploadDate,
        uploadTime
      ];
      
      // Add original data
      headers.forEach(header => {
        temporalRow.push(row[header] || '');
      });
      
      return temporalRow;
    });
    
    // Write data
    if (temporalData.length > 0) {
      sheet.getRange(startRow, 1, temporalData.length, temporalHeaders.length)
        .setValues(temporalData);
      
      // Apply temporal formatting
      applyTemporalFormatting(sheet, startRow, temporalData.length, temporalHeaders.length, temporalInfo);
    }
    
    // Auto-resize columns periodically
    if (startRow % 50 === 2) {
      sheet.autoResizeColumns(1, Math.min(temporalHeaders.length, 12));
    }
    
    logWithTime(`Updated monthly sheet: ${data.length} rows for ${temporalInfo.month_name} ${temporalInfo.year}`);
    return true;
    
  } catch (error) {
    logWithTime(`Monthly sheet update failed: ${error.message}`, 'ERROR');
    return false;
  }
}

// FIXED TRENDS SYSTEM - FULL DATA FOR MONTH-OVER-MONTH COMPARISON
// All trend sheets contain complete report data with temporal columns

// Replace the updateTrendsSheet function with this version
function updateTrendsSheet(sheetId, sheetName, headers, data, reportType, temporalInfo) {
  try {
    const sheet = getOrCreateSheet(sheetId, sheetName);
    
    // Enhanced headers with temporal info for MoM comparison
    const trendsHeaders = [
      'Year_Month',
      'Month_Name',
      'Quarter', 
      'Year',
      'Upload_Date',
      'Upload_Time',
      ...headers // Include ALL original headers for full comparison
    ];
    
    let startRow = 1;
    
    // Initialize sheet if empty
    if (sheet.getLastRow() === 0) {
      sheet.getRange(1, 1, 1, trendsHeaders.length).setValues([trendsHeaders]);
      formatTrendsHeaders(sheet, trendsHeaders.length);
      startRow = 2;
    } else {
      // Append mode - add data for this month
      startRow = sheet.getLastRow() + 1;
    }
    
    // Prepare trends data with temporal info + ALL original data
    const uploadDate = formatDate(new Date());
    const uploadTime = new Date().toLocaleTimeString();
    
    const trendsData = data.map(row => {
      const trendsRow = [
        temporalInfo.year_month,        // 2025-07
        temporalInfo.month_name,        // Jul
        `Q${temporalInfo.quarter}`,     // Q3
        temporalInfo.year,              // 2025
        uploadDate,                     // 2025-09-03
        uploadTime                      // 14:30:15
      ];
      
      // Add ALL original data for full comparison capability
      headers.forEach(header => {
        trendsRow.push(row[header] || '');
      });
      
      return trendsRow;
    });
    
    // Write all data to trends sheet
    if (trendsData.length > 0) {
      sheet.getRange(startRow, 1, trendsData.length, trendsHeaders.length)
        .setValues(trendsData);
      
      // Apply month-based formatting for easy visual comparison
      applyMonthlyTrendsFormatting(sheet, startRow, trendsData.length, trendsHeaders.length, temporalInfo);
    }
    
    // Auto-resize columns periodically
    if (startRow % 100 === 2) {
      sheet.autoResizeColumns(1, Math.min(trendsHeaders.length, 15));
    }
    
    logWithTime(`Updated ${reportType} trends: ${data.length} records for ${temporalInfo.month_name} ${temporalInfo.year}`);
    return true;
    
  } catch (error) {
    logWithTime(`Trends sheet update failed for ${reportType}: ${error.message}`, 'ERROR');
    return false;
  }
}
// Calculate key metrics for monthly aggregation
function calculateMonthlyMetrics(data, reportType) {
  const metrics = {};
  
  try {
    switch (reportType.toLowerCase()) {
      case 'campaign performance':
        metrics.Total_Campaigns = data.length;
        metrics.Total_Impressions = data.reduce((sum, row) => sum + parseInt(row['Impressions'] || 0), 0);
        metrics.Total_Clicks = data.reduce((sum, row) => sum + parseInt(row['Clicks'] || 0), 0);
        metrics.Total_Spend = data.reduce((sum, row) => sum + parseFloat(row['Total Spent'] || 0), 0);
        metrics.Avg_CTR = metrics.Total_Impressions > 0 ? (metrics.Total_Clicks / metrics.Total_Impressions * 100).toFixed(2) : 0;
        metrics.Avg_CPC = metrics.Total_Clicks > 0 ? (metrics.Total_Spend / metrics.Total_Clicks).toFixed(2) : 0;
        break;
        
      case 'content analytics':
        metrics.Total_Posts = data.length;
        metrics.Total_Impressions = data.reduce((sum, row) => sum + parseInt(row['Impressions'] || 0), 0);
        metrics.Total_Engagement = data.reduce((sum, row) => {
          const likes = parseInt(row['Likes'] || 0);
          const comments = parseInt(row['Comments'] || 0);
          const shares = parseInt(row['Shares'] || row['Reposts'] || 0);
          return sum + likes + comments + shares;
        }, 0);
        metrics.Avg_Engagement_Rate = metrics.Total_Impressions > 0 ? 
          (metrics.Total_Engagement / metrics.Total_Impressions * 100).toFixed(2) : 0;
        break;
        
      case 'follower metrics':
        metrics.New_Followers = data.reduce((sum, row) => sum + parseInt(row['Total followers'] || 0), 0);
        metrics.Organic_Followers = data.reduce((sum, row) => sum + parseInt(row['Organic followers'] || 0), 0);
        metrics.Sponsored_Followers = data.reduce((sum, row) => sum + parseInt(row['Sponsored followers'] || 0), 0);
        break;
        
      case 'visitor metrics':
        metrics.Total_Visitors = data.reduce((sum, row) => sum + parseInt(row['Total visitors'] || 0), 0);
        metrics.Page_Views = data.reduce((sum, row) => sum + parseInt(row['Page views'] || 0), 0);
        break;
        
      default:
        // Generic metrics for demographics and other types
        metrics.Total_Records = data.length;
        metrics.Total_Impressions = data.reduce((sum, row) => sum + parseInt(row['Impressions'] || 0), 0);
        metrics.Total_Clicks = data.reduce((sum, row) => sum + parseInt(row['Clicks'] || 0), 0);
    }
    
  } catch (error) {
    logWithTime(`Metrics calculation failed: ${error.message}`, 'ERROR');
    metrics.Total_Records = data.length;
  }
  
  return metrics;
}

// Enhanced processing functions with temporal tracking

// FIXED: Campaign processing with correct period dates from filename
// FIXED: Campaign processing with correct period dates from filename
// FIXED: Campaign processing with correct period dates from filename
function processCampaignFileEnhanced(file, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName); // Uses filename date
    
    logWithTime(`Processing campaign file: ${fileName} (Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}) [Date source: ${periodInfo.date_source}]`);
    
    // Read and process campaign file (same as existing)
    let content;
    try {
      content = file.getBlob().getDataAsString('UTF-16LE');
    } catch (e1) {
      try {
        content = file.getBlob().getDataAsString('UTF-8');
      } catch (e2) {
        try {
          content = file.getBlob().getDataAsString();
        } catch (e3) {
          logWithTime(`Failed to read campaign file: ${e3.message}`, 'ERROR');
          return false;
        }
      }
    }
    
    const rows = parseCSVSafely(content, { delimiter: '\t' });
    if (!rows) return false;
    
    const expectedHeaders = ['Campaign Name', 'Impressions', 'Clicks', 'Total Spent', 'CTR'];
    const headerIdx = findHeaderRowIndex(rows, expectedHeaders);
    const data = convertRowsToObjects(rows, headerIdx);
    
    if (data.length === 0) {
      logWithTime('No campaign data found', 'WARN');
      return false;
    }
    
    // FIXED: Enhanced data with period boundary dates FROM FILENAME
    const enrichedData = data.map(row => ({
      ...row,
      'Period Start Date': periodInfo.period_start_date,
      'Period End Date': periodInfo.period_end_date,
      'Month': periodInfo.month_name,
      'Year': periodInfo.year,
      'Year_Month': periodInfo.year_month,
      'Quarter': `Q${periodInfo.quarter}`,
      'Days in Period': periodInfo.days_in_period,
      'Report Date': periodInfo.original, // Use extracted date, not processing date
      'Processing Date': formatDate(new Date()), // Keep processing date separate
      'Date Source': periodInfo.date_source, // Track where date came from
      'Week': periodInfo.week
    }));
    
    const headers = Object.keys(enrichedData[0]);
    
    // Update existing detailed campaign sheet with period dates
    const success = updateSheetOptimized(
      CONFIG.CAMPAIGN_PERFORMANCE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      'Campaign Performance'
    );
    
    if (success) {
      processingState.data.set('campaign_performance', {
        summary: calculateCampaignSummary(enrichedData),
        date: periodInfo, // Use period info instead of fileDate
        period: periodInfo,
        count: enrichedData.length
      });
      logWithTime(`✓ Campaign Performance updated with filename period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Campaign processing error: ${error.message}`, 'ERROR');
    return false;
  }
}


// ENHANCED FUNCTION: Update the main router to use new processing
function processFollowerFile(file, fileType, fileDate) {
  // Route to the enhanced processing function
  return processFollowerFileEnhancedAppend(file, fileType, fileDate);
}

// LOOKER STUDIO CALCULATED FIELD HELPERS
// Add these comments in your code for Looker Studio setup:

/*
LOOKER STUDIO CALCULATED FIELDS:

1. Smart Demographics Display:
CASE 
  WHEN Date_Range_Type = "single_month" THEN Segment_Percentage
  WHEN Date_Range_Type = "multi_month" AND Is_Latest_Available = true THEN Segment_Percentage
  ELSE NULL
END

2. Latest Data Filter:
CASE 
  WHEN Is_Latest_Available = true THEN Segment_Followers
  ELSE NULL
END

3. Month-over-Month Change:
CASE 
  WHEN LAG(Segment_Percentage, 1, Year_Month) IS NOT NULL 
  THEN Segment_Percentage - LAG(Segment_Percentage, 1, Year_Month)
  ELSE NULL
END

4. Date Range Context:
CASE 
  WHEN COUNT(DISTINCT Year_Month) = 1 THEN "single_month"
  ELSE "multi_month"
END
*/

logWithTime('Enhanced Follower Demographics processing loaded with Looker Studio optimization');

// Enhanced visitor temporal processing
// UPDATED: Visitor processing with append functionality
function processVisitorFileEnhancedAppend(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing visitor file (APPEND MODE): ${fileName} (${fileType}) - Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date} [Date source: ${periodInfo.date_source}]`);
    
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime(`No visitor data found in ${fileType}`, 'WARN');
      return false;
    }
    
    // Enhanced data with period dates and date normalization
    const enrichedData = data.map(row => {
      const processedRow = { ...row };
      
      // Date normalization for any Date columns (same as existing)
      Object.keys(processedRow).forEach(key => {
        const keyLower = key.toLowerCase();
        if (keyLower.includes('date') || keyLower.includes('time')) {
          const originalValue = processedRow[key];
          if (originalValue && typeof originalValue === 'string') {
            processedRow[`Original_${key}`] = originalValue;
            
            try {
              const parsedDate = parseVisitorDate(originalValue);
              if (parsedDate) {
                processedRow[key] = formatDate(parsedDate);
                processedRow[`Normalized_${key}`] = formatDate(parsedDate);
              } else {
                processedRow[key] = originalValue;
                processedRow[`${key}_Parse_Status`] = 'Failed to parse';
              }
            } catch (dateError) {
              processedRow[key] = originalValue;
              processedRow[`${key}_Parse_Status`] = 'Parse error';
            }
          }
        }
      });
      
      // Add period boundary dates FROM FILENAME
      processedRow['Period Start Date'] = periodInfo.period_start_date;
      processedRow['Period End Date'] = periodInfo.period_end_date;
      processedRow['Month'] = periodInfo.month_name;
      processedRow['Year'] = periodInfo.year;
      processedRow['Year_Month'] = periodInfo.year_month;
      processedRow['Quarter'] = `Q${periodInfo.quarter}`;
      processedRow['Days in Period'] = periodInfo.days_in_period;
      processedRow['Report Date'] = periodInfo.original;
      processedRow['Processing Date'] = formatDate(new Date());
      processedRow['Date Source'] = periodInfo.date_source;
      processedRow['File Type'] = fileType;
      processedRow['Week'] = periodInfo.week;
      
      return processedRow;
    });
    
    const sheetId = getVisitorSheetId(fileType);
    if (!sheetId) {
      logWithTime(`No sheet ID found for ${fileType}`, 'ERROR');
      return false;
    }
    
    const headers = Object.keys(enrichedData[0]);
    
    // CHANGED: Use append instead of replace
    const success = updateSheetAppend(
      sheetId,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      `Visitor: ${fileType}`
    );
    
    if (success && fileType === 'visitor_metrics') {
      processingState.data.set('visitor_metrics', {
        summary: calculateVisitorSummary(enrichedData),
        date: periodInfo,
        period: periodInfo,
        count: enrichedData.length
      });
    }
    
    if (success) {
      logWithTime(`✓ ${fileType} appended with filename period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Visitor processing error (${fileType}): ${error.message}`, 'ERROR');
    return false;
  }
}
// Enhanced content temporal processing
function processContentFileTemporal(file, fileDate) {
  try {
    const fileName = file.getName();
    const temporalInfo = extractTemporalInfo(fileName);
    
    logWithTime(`Processing content file with temporal tracking: ${fileName}`);
    
    // Process content file normally first
    const success = processContentFile(file, fileDate);
    if (!success) return false;
    
    // For trends, re-read and process the content data
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return success;
    
    const contentHeaders = [
      'post title', 'impressions', 'clicks', 'likes', 'comments', 'reposts',
      'engagement', 'views', 'ctr'
    ];
    
    const headerIdx = findHeaderRowIndex(rows, contentHeaders);
    const data = convertRowsToObjects(rows, headerIdx);
    
    if (data.length === 0) return success;
    
    // Enhanced data with temporal metadata
    const enrichedData = data.map(row => ({
      ...row,
      'Report Date': fileDate.original || formatDate(new Date()),
      'Processing Date': formatDate(new Date()),
      'Week': fileDate.week || getCurrentDateInfo().week,
      'Year': fileDate.year || getCurrentDateInfo().year
    }));
    
    // Update Content Trends sheet with full data
    if (ENHANCED_CONFIG.COMPARISON_SHEETS?.CONTENT_TRENDS) {
      const headers = Object.keys(enrichedData[0]);
      
      const trendsSuccess = updateTrendsSheet(
        ENHANCED_CONFIG.COMPARISON_SHEETS.CONTENT_TRENDS,
        CONFIG.DEFAULT_SHEET_NAME,
        headers,
        enrichedData,
        'Content Trends',
        temporalInfo
      );
      
      if (trendsSuccess) {
        logWithTime(`✓ Updated Content Trends sheet: ${enrichedData.length} records for ${temporalInfo.month_name} ${temporalInfo.year}`);
      }
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Content temporal processing error: ${error.message}`, 'ERROR');
    return false;
  }
}

// Enhanced competitor temporal processing
function processCompetitorFileTemporal(file, fileDate) {
  try {
    const fileName = file.getName();
    const temporalInfo = extractTemporalInfo(fileName);
    
    logWithTime(`Processing competitor file with temporal tracking: ${fileName}`);
    
    // Read and process competitor file
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime('No competitor data found', 'WARN');
      return false;
    }
    
    // Enhanced data with temporal metadata
    const enrichedData = data.map(row => ({
      ...row,
      'Report Date': fileDate.original || formatDate(new Date()),
      'Processing Date': formatDate(new Date()),
      'Week': fileDate.week || getCurrentDateInfo().week,
      'Year': fileDate.year || getCurrentDateInfo().year
    }));
    
    const headers = Object.keys(enrichedData[0]);
    
    // 1. Update detailed competitor sheet (existing functionality)
    const detailedSuccess = updateSheetOptimized(
      CONFIG.COMPETITOR_PERFORMANCE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      'Competitor Analytics'
    );
    
    // 2. Update Competitor Trends sheet with full data
    let trendsSuccess = false;
    if (detailedSuccess && ENHANCED_CONFIG.COMPARISON_SHEETS?.COMPETITOR_TRENDS) {
      trendsSuccess = updateTrendsSheet(
        ENHANCED_CONFIG.COMPARISON_SHEETS.COMPETITOR_TRENDS,
        CONFIG.DEFAULT_SHEET_NAME,
        headers,
        enrichedData,
        'Competitor Trends',
        temporalInfo
      );
      
      if (trendsSuccess) {
        logWithTime(`✓ Updated Competitor Trends sheet: ${enrichedData.length} records for ${temporalInfo.month_name} ${temporalInfo.year}`);
      }
    }
    
    if (detailedSuccess) {
      processingState.data.set('competitor_analytics', {
        summary: calculateCompetitorSummary(enrichedData),
        temporal: temporalInfo,
        date: fileDate,
        count: enrichedData.length
      });
    }
    
    return detailedSuccess;
    
  } catch (error) {
    logWithTime(`Competitor temporal processing error: ${error.message}`, 'ERROR');
    return false;
  }
}
// UPDATED: Demographics processing with append functionality
function processDemographicsFileEnhancedAppend(file, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing demographics file (APPEND MODE): ${fileName} (Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}) [Date source: ${periodInfo.date_source}]`);
    
    if (!file) {
      logWithTime('Demographics file is null or undefined', 'ERROR');
      return false;
    }
    
    let content;
    try {
      content = file.getBlob().getDataAsString('UTF-8');
    } catch (e1) {
      try {
        content = file.getBlob().getDataAsString();
      } catch (e2) {
        logWithTime(`Failed to read demographics file: ${e2.message}`, 'ERROR');
        return false;
      }
    }
    
    const lines = content.split('\n').map(line => line.trim()).filter(line => line.length > 0);
    const sections = parseSimpleDemographics(lines);
    
    logWithTime(`Found ${sections.length} demographic sections`);
    
    let successCount = 0;
    
    // Process each demographics section with period dates FROM FILENAME
    for (const section of sections) {
      if (!section.data || section.data.length === 0) {
        logWithTime(`Empty section: ${section.type}`, 'WARN');
        continue;
      }
      
      const sheetId = getDemographicsSheetId(section.type);
      const targetSheetId = sheetId || CONFIG.DEMOGRAPHICS_OVERVIEW_SHEET_ID;
      
      // Enhanced data with period boundary dates FROM FILENAME
      const enrichedData = section.data.map(row => ({
        ...row,
        'Period Start Date': periodInfo.period_start_date,
        'Period End Date': periodInfo.period_end_date,
        'Month': periodInfo.month_name,
        'Year': periodInfo.year,
        'Year_Month': periodInfo.year_month,
        'Quarter': `Q${periodInfo.quarter}`,
        'Days in Period': periodInfo.days_in_period,
        'Report Date': periodInfo.original,
        'Processing Date': formatDate(new Date()),
        'Date Source': periodInfo.date_source,
        'Section Type': section.type,
        'Week': periodInfo.week
      }));
      
      const headers = Object.keys(enrichedData[0] || {});
      if (headers.length === 0) continue;
      
      // CHANGED: Use append instead of replace
      const success = updateSheetAppend(
        targetSheetId,
        CONFIG.DEFAULT_SHEET_NAME,
        headers,
        enrichedData,
        `Demographics: ${section.type}`
      );
      
      if (success) {
        successCount++;
        logWithTime(`✓ ${section.type} appended with filename period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date} (${enrichedData.length} rows)`);
      }
    }
    
    if (successCount > 0) {
      processingState.data.set('demographics', {
        summary: { sections_processed: successCount, total_sections: sections.length },
        date: periodInfo,
        period: periodInfo,
        count: sections.reduce((sum, section) => sum + (section.data?.length || 0), 0)
      });
    }
    
    return successCount > 0;
    
  } catch (error) {
    logWithTime(`Demographics processing error: ${error.message}`, 'ERROR');
    return false;
  }
}


function formatTemporalHeaders(sheet, colCount) {
  try {
    sheet.getRange(1, 1, 1, colCount)
      .setFontWeight('bold')
      .setBackground('#0d47a1')
      .setFontColor('white')
      .setHorizontalAlignment('center')
      .setWrap(true);
      
    sheet.setFrozenRows(1);
    
  } catch (error) {
    logWithTime(`Temporal header formatting failed: ${error.message}`, 'WARN');
  }
}

function formatTrendsHeaders(sheet, colCount) {
  try {
    sheet.getRange(1, 1, 1, colCount)
      .setFontWeight('bold')
      .setBackground('#2e7d32')
      .setFontColor('white')
      .setHorizontalAlignment('center');
      
    sheet.setFrozenRows(1);
    
  } catch (error) {
    logWithTime(`Trends header formatting failed: ${error.message}`, 'WARN');
  }
}

function applyTemporalFormatting(sheet, startRow, numRows, numCols, temporalInfo) {
  try {
    // Color coding by age of data
    let bgColor = '#ffffff'; // Current month: white
    
    if (temporalInfo.months_ago === 1) {
      bgColor = '#f3e5f5'; // Previous month: light purple
    } else if (temporalInfo.months_ago >= 2 && temporalInfo.months_ago <= 3) {
      bgColor = '#e8f5e8'; // 2-3 months ago: light green
    } else if (temporalInfo.months_ago >= 4) {
      bgColor = '#fff3e0'; // 4+ months ago: light orange
    }
    
    if (numRows > 0 && numCols > 0) {
      sheet.getRange(startRow, 1, numRows, numCols).setBackground(bgColor);
    }
    
    // Highlight temporal info columns
    if (numRows > 0) {
      sheet.getRange(startRow, 1, numRows, 5).setFontWeight('bold'); // First 5 columns are temporal
    }
    
  } catch (error) {
    logWithTime(`Temporal formatting failed: ${error.message}`, 'WARN');
  }
}

function applyTrendsFormatting(sheet, startRow, numRows, numCols, temporalInfo) {
  try {
    // Highlight current month in trends
    if (temporalInfo.is_current_month) {
      sheet.getRange(startRow, 1, numRows, numCols)
        .setBackground('#c8e6c9')
        .setFontWeight('bold');
    }
    
  } catch (error) {
    logWithTime(`Trends formatting failed: ${error.message}`, 'WARN');
  }
}

// Enhanced master dashboard with temporal insights
function updateMonthlyComparisonDashboard() {
  try {
    logWithTime('Updating monthly comparison dashboard');
    
    if (!ENHANCED_CONFIG.COMPARISON_SHEETS?.MONTHLY_COMPARISON_DASHBOARD) {
      logWithTime('Monthly comparison dashboard sheet ID not configured', 'WARN');
      return false;
    }
    
    const sheet = getOrCreateSheet(
      ENHANCED_CONFIG.COMPARISON_SHEETS.MONTHLY_COMPARISON_DASHBOARD, 
      CONFIG.DEFAULT_SHEET_NAME
    );
    
    // Collect temporal data
    const temporalSummary = [];
    const currentDate = getCurrentDateInfo();
    
    for (const [key, data] of processingState.data.entries()) {
      if (data.temporal) {
        temporalSummary.push({
          type: key.replace('_temporal', ''),
          month: data.temporal.month_name,
          year: data.temporal.year,
          period: data.temporal.year_month,
          count: data.count,
          monthsAgo: data.temporal.months_ago
        });
      }
    }
    
    const dashboardData = [
      ['LinkedIn Analytics - Monthly Trends Dashboard', '', '', formatDate(new Date())],
      ['Last Updated', formatDate(new Date()), `Data Points: ${temporalSummary.length}`, `${new Date().toLocaleTimeString()}`],
      ['', '', '', ''],
      ['RECENT UPLOADS', 'Month', 'Records', 'Type'],
      ...temporalSummary
        .sort((a, b) => a.monthsAgo - b.monthsAgo)
        .slice(0, 10)
        .map(s => [s.period, s.month, s.count, s.type]),
      ['', '', '', ''],
      ['TEMPORAL INSIGHTS', 'Metric', 'Value', 'Trend'],
      ['Current Month Data', temporalSummary.filter(s => s.monthsAgo === 0).length, '', 'Recent'],
      ['Previous Month Data', temporalSummary.filter(s => s.monthsAgo === 1).length, '', 'Historical'],
      ['Total Months Tracked', new Set(temporalSummary.map(s => s.period)).size, '', 'Coverage'],
      ['', '', '', ''],
      ['COMPARISON CAPABILITIES', 'Available', 'Sheet', 'Purpose'],
      ['Monthly Trends', 'Yes', 'Campaign/Content/Follower Trends', 'Month-over-month analysis'],
      ['Quarterly Analysis', 'Yes', 'All trend sheets', 'Seasonal patterns'],
      ['Year-over-Year', 'Yes', 'Historical data', 'Annual growth tracking'],
      ['Custom Periods', 'Yes', 'Filter by date ranges', 'Flexible analysis']
    ];
    
    // Clear and update
    sheet.clear();
    sheet.getRange(1, 1, dashboardData.length, 4).setValues(dashboardData);
    
    // Format dashboard
    formatComparisonDashboard(sheet, dashboardData.length);
    
    logWithTime('Monthly comparison dashboard updated');
    return true;
    
  } catch (error) {
    logWithTime(`Monthly dashboard update failed: ${error.message}`, 'ERROR');
    return false;
  }
}
// Apply month-based formatting to trends sheets for easy comparison
function applyMonthlyTrendsFormatting(sheet, startRow, numRows, numCols, temporalInfo) {
  try {
    // Color coding by month for easy visual comparison
    const monthColors = {
      'Jan': '#ffebee', 'Feb': '#fce4ec', 'Mar': '#f3e5f5', 'Apr': '#ede7f6',
      'May': '#e8eaf6', 'Jun': '#e3f2fd', 'Jul': '#e1f5fe', 'Aug': '#e0f2f1',
      'Sep': '#e8f5e8', 'Oct': '#f1f8e9', 'Nov': '#f9fbe7', 'Dec': '#fffde7'
    };
    
    const bgColor = monthColors[temporalInfo.month_name] || '#ffffff';
    
    if (numRows > 0 && numCols > 0) {
      sheet.getRange(startRow, 1, numRows, numCols).setBackground(bgColor);
    }
    
    // Bold the temporal columns for easy identification
    if (numRows > 0 && numCols >= 6) {
      sheet.getRange(startRow, 1, numRows, 6).setFontWeight('bold'); // First 6 columns are temporal
    }
    
  } catch (error) {
    logWithTime(`Monthly trends formatting failed: ${error.message}`, 'WARN');
  }
}

function parseCSVSafely(content, options = {}) {
  try {
    if (!content || typeof content !== 'string') {
      throw new Error('Invalid content type');
    }
    
    // Clean content
    content = content.trim();
    if (content.length < 10) {
      throw new Error('Content too short');
    }
    
    // Detect delimiter
    let delimiter = options.delimiter;
    if (!delimiter) {
      const tabCount = (content.match(/\t/g) || []).length;
      const commaCount = (content.match(/,/g) || []).length;
      delimiter = tabCount > commaCount ? '\t' : ',';
      logWithTime(`Auto-detected delimiter: ${delimiter === '\t' ? 'TAB' : 'COMMA'}`);
    }
    
    // Parse with detected/specified delimiter
    let parsed;
    try {
      parsed = Utilities.parseCsv(content, delimiter);
    } catch (parseError) {
      // Fallback to alternate delimiter
      const altDelimiter = delimiter === '\t' ? ',' : '\t';
      logWithTime(`Primary parsing failed, trying ${altDelimiter === '\t' ? 'TAB' : 'COMMA'}`, 'WARN');
      parsed = Utilities.parseCsv(content, altDelimiter);
    }
    
    if (!parsed || parsed.length === 0) {
      throw new Error('No data parsed from CSV');
    }
    
    // Clean and filter rows
    const filtered = parsed
      .map(row => row ? row.map(cell => (cell || '').toString().trim()) : [])
      .filter(row => row && row.some(cell => cell.length > 0));
    
    if (filtered.length < CONSTANTS.MIN_DATA_ROWS + 1) {
      throw new Error(`Insufficient data rows: ${filtered.length}`);
    }
    
    logWithTime(`Parsed CSV: ${filtered.length} rows, ${filtered[0]?.length || 0} columns`);
    return filtered;
    
  } catch (error) {
    logWithTime(`CSV parsing failed: ${error.message}`, 'ERROR');
    return null;
  }
}
// Add this helper function
function sortSheetChronologically(sheetId, sheetName, reportType) {
  try {
    const sheet = getOrCreateSheet(sheetId, sheetName);
    const lastRow = sheet.getLastRow();
    
    if (lastRow > 2) {
      const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      const yearMonthIndex = headers.findIndex(h => h === 'Year_Month') + 1;
      
      if (yearMonthIndex > 0) {
        sheet.getRange(2, 1, lastRow - 1, headers.length).sort(yearMonthIndex);
        logWithTime(`${reportType} sorted in chronological order`);
        return true;
      }
    }
    return false;
  } catch (error) {
    logWithTime(`Chronological sorting failed for ${reportType}: ${error.message}`, 'WARN');
    return false;
  }
}

function findHeaderRowIndex(rows, expectedPatterns) {
  if (!rows || rows.length === 0) return 0;
  
  // Check up to 10 rows for headers (LinkedIn often has metadata in first few rows)
  const maxRowsToCheck = Math.min(10, rows.length);
  
  for (let i = 0; i < maxRowsToCheck; i++) {
    const row = rows[i];
    if (!row) continue;
    
    const rowText = row.join('').toLowerCase();
    let matches = 0;
    
    // Check for common LinkedIn content headers
    const contentHeaders = [
      'post title', 'impressions', 'clicks', 'likes', 'comments', 'reposts',
      'engagement', 'views', 'follows', 'ctr', 'click through rate'
    ];
    
    // Use content headers if no specific patterns provided
    const headersToCheck = expectedPatterns.length > 0 ? expectedPatterns : contentHeaders;
    
    for (const pattern of headersToCheck) {
      if (rowText.includes(pattern.toLowerCase())) {
        matches++;
      }
    }
    
    // More flexible matching - need at least 3 matches or 30% of headers
    const requiredMatches = Math.max(3, Math.ceil(headersToCheck.length * 0.3));
    
    if (matches >= requiredMatches) {
      logWithTime(`Found header at row ${i + 1} with ${matches} matches out of ${headersToCheck.length} possible`);
      logWithTime(`Header row content: ${row.join(', ').substring(0, 200)}...`);
      return i;
    }
  }
  
  logWithTime('No clear header row found, using row 1', 'WARN');
  return 0;
}

function convertRowsToObjects(rows, headerIndex = 0) {
  if (!rows || rows.length <= headerIndex) return [];
  
  const headers = rows[headerIndex]
    .map(h => (h || '').toString().trim())
    .filter(h => h.length > 0);
  
  if (headers.length === 0) {
    logWithTime('No valid headers found', 'ERROR');
    return [];
  }
  
  const dataRows = rows.slice(headerIndex + 1);
  
  const objects = dataRows.map(row => {
    const obj = {};
    headers.forEach((header, index) => {
      obj[header] = (row[index] || '').toString().trim();
    });
    return obj;
  }).filter(obj => {
    // Keep rows with at least one non-empty value
    return Object.values(obj).some(value => value && value.length > 0);
  });
  
  logWithTime(`Converted ${objects.length} data rows to objects`);
  return objects;
}

// ===================================================================
// SHEET OPERATIONS (OPTIMIZED)
// ===================================================================

function getOrCreateSheet(sheetId, sheetName = null) {
  try {
    const spreadsheet = SpreadsheetApp.openById(sheetId);
    let sheet;
    
    if (sheetName) {
      sheet = spreadsheet.getSheetByName(sheetName);
      if (!sheet) {
        logWithTime(`Sheet '${sheetName}' not found, using first sheet`, 'WARN');
        sheet = spreadsheet.getSheets()[0];
      }
    } else {
      sheet = spreadsheet.getSheets()[0]; // Use first available sheet
    }
    
    if (!sheet) {
      throw new Error('No sheets found in spreadsheet');
    }
    
    return sheet;
    
  } catch (error) {
    logWithTime(`Sheet access failed: ${error.message}`, 'ERROR');
    throw error;
  }
}

function updateSheetOptimized(sheetId, sheetName, headers, data, reportType) {
  try {
    if (!data || data.length === 0) {
      logWithTime(`No data to update for ${reportType}`, 'WARN');
      return false;
    }
    
    if (!headers || headers.length === 0) {
      logWithTime(`No headers provided for ${reportType}`, 'ERROR');
      return false;
    }
    
    const sheet = getOrCreateSheet(sheetId, sheetName);
    
    // Clear existing content
    sheet.clear();
    
    // Prepare all data including headers with proper formatting
    const allData = [headers].concat(
      data.map(row => headers.map(header => {
        let value = row[header];
        if (value !== undefined) {
          value = value.toString();
          
          // Fix: Prevent auto-conversion of company sizes and similar patterns to dates
          // Add single quote prefix for values that look like dates but aren't
          if (isLikelyCompanySize(value) || isLikelyNotDate(value)) {
            // Don't add quote - instead we'll set as text after writing
            return value;
          }
          
          return value;
        }
        return '';
      }))
    );
    
    // Validate data dimensions
    if (allData.length > 10000 || headers.length > 100) {
      logWithTime(`Large dataset warning: ${allData.length} rows, ${headers.length} cols`, 'WARN');
    }
    
    // Write data in batches if needed
    if (allData.length > CONFIG.BATCH_SIZE) {
      const batches = Math.ceil(allData.length / CONFIG.BATCH_SIZE);
      logWithTime(`Writing ${reportType} in ${batches} batches`);
      
      for (let i = 0; i < batches; i++) {
        const startRow = i * CONFIG.BATCH_SIZE;
        const endRow = Math.min(startRow + CONFIG.BATCH_SIZE, allData.length);
        const batchData = allData.slice(startRow, endRow);
        
        const range = sheet.getRange(startRow + 1, 1, batchData.length, headers.length);
        range.setValues(batchData);
        
        if (i > 0) Utilities.sleep(100); // Brief pause between batches
      }
    } else {
      // Single write for smaller datasets
      sheet.getRange(1, 1, allData.length, headers.length).setValues(allData);
    }
    
    // Fix: Set number format to text for columns that contain company sizes or similar data
    fixColumnFormatting(sheet, headers, allData, reportType);
    
    // Format sheet
    formatSheet(sheet, reportType, headers.length, allData.length);
    
    logWithTime(`✓ Updated ${reportType}: ${data.length} rows, ${headers.length} columns`);
    return true;
    
  } catch (error) {
    logWithTime(`Sheet update failed for ${reportType}: ${error.message}`, 'ERROR');
    return false;
  }
}

// ===================================================================
// APPEND FUNCTION FOR HISTORICAL DATA BACKFILL WITH AUTO-SORT
// ===================================================================

// Replace the existing updateSheetAppend function with this corrected version
function updateSheetAppend(sheetId, sheetName, headers, data, reportType) {
  try {
    if (!data || data.length === 0) {
      logWithTime(`No data to append for ${reportType}`, 'WARN');
      return false;
    }

    if (!headers || headers.length === 0) {
      logWithTime(`No headers provided for ${reportType}`, 'ERROR');
      return false;
    }

    // Validate headers array
    const validHeaders = headers.filter(h => h && h.toString().trim().length > 0);
    if (validHeaders.length === 0) {
      logWithTime(`All headers are empty for ${reportType}`, 'ERROR');
      return false;
    }

    logWithTime(`Processing ${reportType}: ${data.length} rows, ${validHeaders.length} columns`);
    logWithTime(`Headers: ${validHeaders.slice(0, 10).join(', ')}${validHeaders.length > 10 ? '...' : ''}`);

    const sheet = getOrCreateSheet(sheetId, sheetName);
    const lastRow = sheet.getLastRow();
    
    // If sheet is empty, add headers first
    if (lastRow === 0) {
      logWithTime(`Creating new sheet with headers for ${reportType}`);
      
      // Write headers
      sheet.getRange(1, 1, 1, validHeaders.length).setValues([validHeaders]);
      
      // Format headers
      sheet.getRange(1, 1, 1, validHeaders.length)
        .setFontWeight('bold')
        .setBackground('#1565c0')
        .setFontColor('white')
        .setHorizontalAlignment('center');
      
      sheet.setFrozenRows(1);
      logWithTime(`Headers written: ${validHeaders.join(', ')}`);
    } else {
      // Validate existing headers match
      const existingHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      logWithTime(`Existing headers: ${existingHeaders.slice(0, 10).join(', ')}`);
      
      // If headers don't match, log warning but continue
      if (existingHeaders.length !== validHeaders.length) {
        logWithTime(`Header count mismatch: existing=${existingHeaders.length}, new=${validHeaders.length}`, 'WARN');
      }
    }
    
    // Prepare data rows - ensure all headers are represented
    const dataRows = data.map((row, rowIndex) => {
      const dataRow = validHeaders.map(header => {
        let value = row[header];
        if (value !== undefined && value !== null) {
          value = value.toString();
          // Prevent auto-conversion of company sizes and date-like values
          if (isLikelyCompanySize(value) || isLikelyNotDate(value)) {
            return value;
          }
          return value;
        }
        return '';
      });
      
      // Validate row length
      if (dataRow.length !== validHeaders.length) {
        logWithTime(`Row ${rowIndex} length mismatch: expected=${validHeaders.length}, got=${dataRow.length}`, 'WARN');
      }
      
      return dataRow;
    });
    
    // Append data starting from the next available row
    const startRow = sheet.getLastRow() + 1;
    
    logWithTime(`Writing ${dataRows.length} rows starting at row ${startRow}`);
    
    // Validate data dimensions before writing
    if (dataRows.length > 0 && validHeaders.length > 0) {
      try {
        sheet.getRange(startRow, 1, dataRows.length, validHeaders.length).setValues(dataRows);
        logWithTime(`✓ Data written successfully to ${reportType}`);
        
        // Apply column formatting to new data
        const allData = [validHeaders].concat(dataRows);
        fixColumnFormatting(sheet, validHeaders, allData, reportType);
        
        // AUTO-SORT BY PERIOD START DATE TO MAINTAIN CHRONOLOGICAL ORDER
        const dateColumnIndex = validHeaders.indexOf('Period Start Date');
        if (dateColumnIndex !== -1 && sheet.getLastRow() > 1) {
          try {
            sheet.getRange(2, 1, sheet.getLastRow() - 1, validHeaders.length).sort(dateColumnIndex + 1);
            logWithTime(`✓ Data sorted chronologically by Period Start Date`);
          } catch (sortError) {
            logWithTime(`Sort failed but data was appended: ${sortError.message}`, 'WARN');
          }
        }
        
        // Add alternating row colors for readability (after sorting)
        const finalLastRow = sheet.getLastRow();
        for (let i = 2; i <= finalLastRow; i += 2) {
          try {
            sheet.getRange(i, 1, 1, validHeaders.length).setBackground('#f8f9fa');
          } catch (colorError) {
            // Continue if coloring fails
          }
        }
        
        logWithTime(`✓ Appended ${dataRows.length} rows to ${reportType} and sorted chronologically`);
        
      } catch (writeError) {
        logWithTime(`Failed to write data to sheet: ${writeError.message}`, 'ERROR');
        return false;
      }
    } else {
      logWithTime(`No valid data to write: rows=${dataRows.length}, cols=${validHeaders.length}`, 'ERROR');
      return false;
    }
    
    return true;
    
  } catch (error) {
    logWithTime(`Append failed for ${reportType}: ${error.message}`, 'ERROR');
    logWithTime(`Error details: ${error.stack}`, 'ERROR');
    return false;
  }
}

// ADD THESE THREE FUNCTIONS:

function calculateTotalFollowersFromData(data) {
  return data.reduce((sum, row) => {
    return sum + parseInt(row['Total followers'] || 0); // Changed to lowercase 'f'
  }, 0);
}

// ENHANCED FUNCTION: Better month detection
function isCurrentOrMostRecentMonth(periodInfo) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1;
  
  // Check if this is current month
  if (periodInfo.year === currentYear && periodInfo.month === currentMonth) {
    return true;
  }
  
  // Check if this is the most recent month we have data for
  // (could be previous month if current month data isn't available yet)
  const dataDate = new Date(periodInfo.year, periodInfo.month - 1, 1);
  const currentDate = new Date(currentYear, currentMonth - 1, 1);
  const diffMonths = (currentDate.getFullYear() - dataDate.getFullYear()) * 12 + (currentDate.getMonth() - dataDate.getMonth());
  
  // Consider it "latest" if it's within 1 month of current
  return diffMonths <= 1;
}

function processFollowerDemographicsByType(data, reportType, isLatestPeriod) {
  const totalFollowers = calculateTotalFollowersFromData(data);
  
  if (isLatestPeriod) {
    return data.map(row => {
      const segmentFollowers = parseInt(row['Total followers'] || 0); // Changed to lowercase 'f'
      const currentPercentage = totalFollowers > 0 ? 
        (segmentFollowers / totalFollowers * 100) : 0;
      
      return {
        ...row,
        'Current Total': segmentFollowers,
        'Current Percentage': Math.round(currentPercentage * 100) / 100,
        'Total Follower Base': totalFollowers,
        'Period': 'Latest',
        'Report Type': reportType,
        'Data Type': 'Current Composition'
      };
    });
  } else {
    return data.map(row => ({
      ...row,
      'Historical Total': parseInt(row['Total followers'] || 0), // Changed to lowercase 'f'
      'Historical Percentage': parseFloat(row['Percentage'] || 0),
      'Report Type': reportType,
      'Data Type': 'Historical Reference'
    }));
  }
}

// Helper function to identify values that look like dates but aren't
function isLikelyNotDate(value) {
  if (!value || typeof value !== 'string') return false;
  
  const notDatePatterns = [
    /^\d+-\d+$/,           // Range values like 2-10
    /^\d+\+$/,             // Plus values like 10001+
    /^\d+k\+$/,            // Values like 10k+
    /^\d+K\+$/,            // Values like 10K+
  ];
  
  return notDatePatterns.some(pattern => pattern.test(value.trim()));
}

// Fix column formatting to prevent auto-conversion
// Enhanced fixColumnFormatting function with date formatting
function fixColumnFormatting(sheet, headers, allData, reportType) {
  try {
    if (!allData || allData.length === 0) return;
    
    // Find columns that need specific formatting
    const columnsToFixAsText = [];
    const columnsToFixAsDate = [];
    
    headers.forEach((header, colIndex) => {
      const headerLower = header.toLowerCase();
      
      // Check for date columns
      if (headerLower.includes('date') || headerLower.includes('time')) {
        columnsToFixAsDate.push({
          index: colIndex + 1, // Google Sheets uses 1-based indexing
          header: header
        });
        logWithTime(`Will format column ${colIndex + 1} (${header}) as date`);
      }
      // Check for columns that should be text (company sizes, etc.)
      else if (headerLower.includes('company size') || 
               headerLower.includes('size') ||
               headerLower.includes('range') ||
               headerLower.includes('segment')) {
        
        // Check if any values in this column look like company sizes
        let hasCompanySizes = false;
        for (let rowIndex = 1; rowIndex < Math.min(allData.length, 20); rowIndex++) {
          const cellValue = allData[rowIndex][colIndex];
          if (cellValue && (isLikelyCompanySize(cellValue) || isLikelyNotDate(cellValue))) {
            hasCompanySizes = true;
            break;
          }
        }
        
        if (hasCompanySizes) {
          columnsToFixAsText.push(colIndex + 1);
          logWithTime(`Will format column ${colIndex + 1} (${header}) as text to prevent date conversion`);
        }
      }
    });
    
    // Apply text formatting to identified columns
    columnsToFixAsText.forEach(colNum => {
      try {
        const range = sheet.getRange(1, colNum, allData.length, 1);
        range.setNumberFormat('@'); // @ is the text format
        logWithTime(`Applied text formatting to column ${colNum}`);
      } catch (formatError) {
        logWithTime(`Failed to format column ${colNum} as text: ${formatError.message}`, 'WARN');
      }
    });
    
    // Apply date formatting to date columns
    columnsToFixAsDate.forEach(col => {
      try {
        const range = sheet.getRange(2, col.index, allData.length - 1, 1); // Skip header row
        
        // Try different date formats based on the data
        let dateFormat = 'yyyy-mm-dd'; // Default format
        
        // Check sample data to determine best format
        if (allData.length > 1) {
          const sampleValue = allData[1][col.index - 1];
          if (sampleValue) {
            if (sampleValue.includes('/')) {
              dateFormat = 'mm/dd/yyyy';
            } else if (sampleValue.includes('-') && sampleValue.length > 10) {
              dateFormat = 'yyyy-mm-dd hh:mm:ss'; // For datetime values
            }
          }
        }
        
        range.setNumberFormat(dateFormat);
        logWithTime(`Applied date formatting (${dateFormat}) to column ${col.index} (${col.header})`);
      } catch (formatError) {
        logWithTime(`Failed to format column ${col.index} as date: ${formatError.message}`, 'WARN');
      }
    });
    
  } catch (error) {
    logWithTime(`Column formatting failed: ${error.message}`, 'WARN');
  }
}

// Helper function to detect if a value looks like a proper date
function isLikelyDate(value) {
  if (!value || typeof value !== 'string') return false;
  
  const datePatterns = [
    /^\d{4}-\d{2}-\d{2}$/,           // 2024-12-25
    /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/, // 2024-12-25 14:30:00
    /^\d{2}\/\d{2}\/\d{4}$/,         // 12/25/2024
    /^\d{1,2}\/\d{1,2}\/\d{4}$/,     // 1/5/2024
  ];
  
  return datePatterns.some(pattern => pattern.test(value.trim()));
}

// Enhanced formatSheet function to handle date columns properly
function formatSheet(sheet, reportType, colCount, rowCount) {
  try {
    if (rowCount === 0 || colCount === 0) return;
    
    // Header formatting
    const headerRange = sheet.getRange(1, 1, 1, colCount);
    headerRange
      .setFontWeight('bold')
      .setBackground('#1565c0')
      .setFontColor('white')
      .setHorizontalAlignment('center');
    
    // Auto-resize columns (limit to reasonable number)
    const maxCols = Math.min(colCount, 20);
    sheet.autoResizeColumns(1, maxCols);
    
    // Freeze header row
    sheet.setFrozenRows(1);
    
    // Add borders for better visibility
    if (rowCount > 1) {
      sheet.getRange(1, 1, Math.min(rowCount, 1000), colCount)
        .setBorder(true, true, true, true, true, true);
    }
    
    // Add alternating row colors for better readability
    if (rowCount > 2) {
      for (let i = 2; i <= Math.min(rowCount, 1000); i += 2) {
        sheet.getRange(i, 1, 1, colCount).setBackground('#f8f9fa');
      }
    }
    
  } catch (error) {
    logWithTime(`Sheet formatting failed: ${error.message}`, 'WARN');
  }
}
function processFollowerDemographicsByType(data, reportType, isLatestPeriod) {
  const totalFollowers = calculateTotalFollowersFromData(data);
  
  if (isLatestPeriod) {
    return data.map(row => {
      const segmentFollowers = parseInt(row['Total Followers'] || 0);
      const currentPercentage = totalFollowers > 0 ? 
        (segmentFollowers / totalFollowers * 100) : 0;
      
      return {
        ...row,
        'Current Total': segmentFollowers,
        'Current Percentage': Math.round(currentPercentage * 100) / 100,
        'Total Follower Base': totalFollowers,
        'Period': 'Latest',
        'Report Type': reportType,
        'Data Type': 'Current Composition'
      };
    });
  } else {
    // Historical data for trend analysis only
    return data.map(row => ({
      ...row,
      'Historical Total': parseInt(row['Total Followers'] || 0),
      'Historical Percentage': parseFloat(row['Percentage'] || 0),
      'Report Type': reportType,
      'Data Type': 'Historical Reference'
    }));
  }
}
// ===================================================================
// DEMOGRAPHICS PROCESSING (MOVED UP)
// ===================================================================

// Enhanced parseSimpleDemographics function with better CSV handling
function parseSimpleDemographics(lines) {
  const sections = [];
  
  try {
    let currentSection = null;
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      
      // Detect section header: contains "Segment" and "Impressions"
      if (line.includes('Segment') && line.includes('Impressions')) {
        // Save previous section
        if (currentSection && currentSection.data.length > 0) {
          sections.push(currentSection);
          logWithTime(`Completed section: ${currentSection.type} with ${currentSection.data.length} records`);
        }
        
        // Parse headers more carefully to handle quotes and commas in industry names
        const headers = parseCSVLineSafely(line);
        let sectionType = headers[0];
        
        // Enhanced job titles detection
        if (sectionType.toLowerCase().includes('job title') || 
            sectionType.toLowerCase().includes('job titles')) {
          sectionType = 'Job Title Segment';
          logWithTime(`*** DETECTED JOB TITLES SECTION: "${headers[0]}" -> "${sectionType}"`);
        }
        
        currentSection = {
          type: sectionType,
          headers: headers,
          data: [],
          startLine: i
        };
        
        logWithTime(`Found section: ${sectionType} with ${headers.length} headers`);
        logWithTime(`Headers: ${headers.slice(0, 5).join(', ')}...`);
        
      } else if (currentSection && line.length > 0 && !line.includes('Report') && !line.startsWith(',')) {
        // Parse data row more carefully
        const values = parseCSVLineSafely(line);
        
        // Validate that we have the expected number of columns
        if (values.length !== currentSection.headers.length) {
          logWithTime(`Column count mismatch in ${currentSection.type}: expected ${currentSection.headers.length}, got ${values.length}`, 'WARN');
          logWithTime(`Raw line: ${line.substring(0, 200)}...`);
          logWithTime(`Parsed values: ${values.slice(0, 5).join(' | ')}...`);
          
          // Try to fix column alignment
          const fixedValues = alignColumnValues(values, currentSection.headers.length);
          if (fixedValues.length === currentSection.headers.length) {
            logWithTime(`Fixed column alignment for ${currentSection.type}`);
            values.splice(0, values.length, ...fixedValues);
          } else {
            logWithTime(`Could not fix column alignment, skipping row`, 'ERROR');
            continue;
          }
        }
        
        if (values[0] && values[0].length > 0) {
          const rowData = {};
          let hasValidData = false;
          
          currentSection.headers.forEach((header, index) => {
            const value = values[index] || '';
            rowData[header] = value;
            
            // Consider more data valid (not just non-zero values)
            if (value && value !== '0' && value !== '0%' && value !== '0.000%' && value.trim().length > 0) {
              hasValidData = true;
            }
          });
          
          if (hasValidData) {
            currentSection.data.push(rowData);
            
            // Enhanced job titles logging
            if (currentSection.type.toLowerCase().includes('job title')) {
              logWithTime(`Job Title record: ${values[0]} -> ${Object.keys(rowData).length} fields`);
            }
            
            // Debug logging for industry segment to catch alignment issues
            if (currentSection.type.toLowerCase().includes('industry')) {
              const impressionsValue = rowData['Impressions'] || rowData['impressions'];
              if (impressionsValue && isNaN(impressionsValue.replace(/,/g, ''))) {
                logWithTime(`Potential data alignment issue in Industry: "${values[0]}" has non-numeric impressions: "${impressionsValue}"`, 'WARN');
              }
            }
          }
        }
      }
    }
    
    // Don't forget the last section
    if (currentSection && currentSection.data.length > 0) {
      sections.push(currentSection);
      logWithTime(`Completed final section: ${currentSection.type} with ${currentSection.data.length} records`);
    }
    
    logWithTime(`Demographics parsing completed: ${sections.length} sections total`);
    
  } catch (error) {
    logWithTime(`Demographics parsing error: ${error.message}`, 'ERROR');
  }
  
  return sections;
}

// Enhanced CSV line parsing to handle quotes and commas properly
function parseCSVLineSafely(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  let i = 0;
  
  while (i < line.length) {
    const char = line[i];
    
    if (char === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        // Handle escaped quotes ""
        current += '"';
        i += 2;
      } else {
        // Toggle quote state
        inQuotes = !inQuotes;
        i++;
      }
    } else if (char === ',' && !inQuotes) {
      // Found delimiter outside quotes
      values.push(current.trim());
      current = '';
      i++;
    } else {
      current += char;
      i++;
    }
  }
  
  // Add the last value
  values.push(current.trim());
  
  // Clean up values - remove surrounding quotes if present
  return values.map(value => {
    if (value.startsWith('"') && value.endsWith('"') && value.length > 1) {
      return value.slice(1, -1);
    }
    return value;
  });
}

// Function to attempt column alignment when there's a mismatch
function alignColumnValues(values, expectedLength) {
  if (values.length === expectedLength) return values;
  
  // If we have too many values, it might be because an industry name contained a comma
  if (values.length > expectedLength) {
    // Try to merge values that might have been split incorrectly
    const fixed = [...values];
    
    // Look for the first numeric value (likely impressions)
    let firstNumericIndex = -1;
    for (let i = 1; i < fixed.length; i++) {
      const val = fixed[i].replace(/,/g, ''); // Remove thousand separators
      if (!isNaN(val) && val.length > 0) {
        firstNumericIndex = i;
        break;
      }
    }
    
    if (firstNumericIndex > 1) {
      // Merge everything before the first numeric value into the first column (industry name)
      const mergedIndustry = fixed.slice(0, firstNumericIndex).join(', ');
      const remainingValues = fixed.slice(firstNumericIndex);
      
      const result = [mergedIndustry, ...remainingValues];
      
      if (result.length === expectedLength) {
        logWithTime(`Successfully merged industry name: "${mergedIndustry}"`);
        return result;
      }
    }
  }
  
  // If we have too few values, pad with empty strings
  if (values.length < expectedLength) {
    const padded = [...values];
    while (padded.length < expectedLength) {
      padded.push('');
    }
    return padded;
  }
  
  // If we can't fix it, return original
  return values;
}

// Enhanced processDemographicsFile function with better error handling
// Enhanced demographics processing - adds period dates to existing detailed sheets
function processDemographicsFileEnhanced(file, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing demographics file: ${fileName} (Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date})`);
    
    if (!file) {
      logWithTime('Demographics file is null or undefined', 'ERROR');
      return false;
    }
    
    // Read and parse file (same as existing)
    let content;
    try {
      content = file.getBlob().getDataAsString('UTF-8');
      logWithTime('Successfully read demographics file with UTF-8 encoding');
    } catch (e1) {
      try {
        content = file.getBlob().getDataAsString();
        logWithTime('Successfully read demographics file with default encoding');
      } catch (e2) {
        logWithTime(`Failed to read demographics file: ${e2.message}`, 'ERROR');
        return false;
      }
    }
    
    const lines = content.split('\n').map(line => line.trim()).filter(line => line.length > 0);
    const sections = parseSimpleDemographics(lines);
    
    logWithTime(`Found ${sections.length} demographic sections`);
    
    let successCount = 0;
    
    // Process each demographics section with period dates
    for (const section of sections) {
      if (!section.data || section.data.length === 0) {
        logWithTime(`Empty section: ${section.type}`, 'WARN');
        continue;
      }
      
      const sheetId = getDemographicsSheetId(section.type);
      const targetSheetId = sheetId || CONFIG.DEMOGRAPHICS_OVERVIEW_SHEET_ID;
      
      // Enhanced data with period boundary dates
      const enrichedData = section.data.map(row => ({
        ...row,
        'Period Start Date': periodInfo.period_start_date,
        'Period End Date': periodInfo.period_end_date,
        'Month': periodInfo.month_name,
        'Year': periodInfo.year,
        'Year_Month': periodInfo.year_month,
        'Quarter': `Q${periodInfo.quarter}`,
        'Days in Period': periodInfo.days_in_period,
        'Report Date': fileDate.original || formatDate(new Date()),
        'Processing Date': formatDate(new Date()),
        'Section Type': section.type,
        'Week': fileDate.week || getCurrentDateInfo().week
      }));
      
      const headers = Object.keys(enrichedData[0] || {});
      if (headers.length === 0) continue;
      
      // Update existing detailed demographics sheet with period dates
      const success = updateSheetOptimized(
        targetSheetId,
        CONFIG.DEFAULT_SHEET_NAME,
        headers,
        enrichedData,
        `Demographics: ${section.type}`
      );
      
      if (success) {
        successCount++;
        logWithTime(`✓ ${section.type} updated with period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date} (${enrichedData.length} rows)`);
      }
    }
    
    if (successCount > 0) {
      processingState.data.set('demographics', {
        summary: { sections_processed: successCount, total_sections: sections.length },
        date: fileDate,
        period: periodInfo,
        count: sections.reduce((sum, section) => sum + (section.data?.length || 0), 0)
      });
    }
    
    return successCount > 0;
    
  } catch (error) {
    logWithTime(`Demographics processing error: ${error.message}`, 'ERROR');
    return false;
  }
  if (success) {
  // Add sorting call here
  sortSheetChronologically(targetSheetId, CONFIG.DEFAULT_SHEET_NAME, `Demographics: ${section.type}`);
  
  successCount++;
  logWithTime(`✓ ${section.type} updated with period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date} (${enrichedData.length} rows)`);
}
}

// Function to validate section data integrity
function validateSectionData(section) {
  try {
    if (!section.data || section.data.length === 0) {
      return { valid: false, error: 'No data in section' };
    }
    
    // Check that numeric columns contain numeric data
    const numericColumns = ['Impressions', 'Clicks', 'Conversions', 'Sends', 'Opens'];
    let invalidDataCount = 0;
    
    for (const row of section.data) {
      for (const col of numericColumns) {
        const value = row[col];
        if (value && value !== '0' && value !== '--') {
          const numericValue = value.replace(/,/g, ''); // Remove thousand separators
          if (isNaN(numericValue)) {
            logWithTime(`Invalid numeric data in ${section.type}, column ${col}: "${value}"`, 'WARN');
            invalidDataCount++;
          }
        }
      }
    }
    
    // Allow some invalid data but not too much
    const invalidRatio = invalidDataCount / (section.data.length * numericColumns.length);
    if (invalidRatio > 0.1) { // More than 10% invalid data
      return { valid: false, error: `Too much invalid numeric data: ${(invalidRatio * 100).toFixed(1)}%` };
    }
    
    return { valid: true };
    
  } catch (error) {
    return { valid: false, error: error.message };
  }
}
// ===================================================================
// FILE DEBUG AND VALIDATION FUNCTIONS
// ===================================================================

function debugFileAccess(file, context = 'unknown') {
  const debugInfo = {
    context,
    timestamp: new Date().toISOString(),
    file: null,
    error: null
  };
  
  try {
    if (!file) {
      debugInfo.error = 'File object is null or undefined';
      return debugInfo;
    }
    
    debugInfo.file = {
      hasGetName: typeof file.getName === 'function',
      hasGetBlob: typeof file.getBlob === 'function',
      hasMoveTo: typeof file.moveTo === 'function'
    };
    
    // Test getName
    try {
      const name = file.getName();
      debugInfo.file.name = name;
      debugInfo.file.nameLength = name ? name.length : 0;
    } catch (nameError) {
      debugInfo.file.nameError = nameError.message;
    }
    
    // Test getSize
    try {
      const size = file.getSize();
      debugInfo.file.size = size;
    } catch (sizeError) {
      debugInfo.file.sizeError = sizeError.message;
    }
    
    // Test basic blob access
    try {
      const blob = file.getBlob();
      debugInfo.file.blobExists = !!blob;
      debugInfo.file.blobSize = blob ? blob.getSize() : 0;
    } catch (blobError) {
      debugInfo.file.blobError = blobError.message;
    }
    
  } catch (generalError) {
    debugInfo.error = generalError.message;
  }
  
  return debugInfo;
}

function validateFileObject(file, context = 'processing') {
  try {
    if (!file) {
      logWithTime(`File validation failed: null file in ${context}`, 'ERROR');
      return { valid: false, error: 'File is null' };
    }
    
    // Check if it has required methods
    if (typeof file.getName !== 'function') {
      logWithTime(`File validation failed: no getName method in ${context}`, 'ERROR');
      return { valid: false, error: 'File missing getName method' };
    }
    
    if (typeof file.getBlob !== 'function') {
      logWithTime(`File validation failed: no getBlob method in ${context}`, 'ERROR');
      return { valid: false, error: 'File missing getBlob method' };
    }
    
    // Test if we can access basic properties
    try {
      const name = file.getName();
      if (!name || name.length === 0) {
        logWithTime(`File validation failed: empty name in ${context}`, 'ERROR');
        return { valid: false, error: 'File has empty name' };
      }
    } catch (nameError) {
      logWithTime(`File validation failed: getName error in ${context}: ${nameError.message}`, 'ERROR');
      return { valid: false, error: `Cannot get filename: ${nameError.message}` };
    }
    
    return { valid: true };
    
  } catch (error) {
    logWithTime(`File validation exception in ${context}: ${error.message}`, 'ERROR');
    return { valid: false, error: error.message };
  }
}

// ===================================================================
// ENHANCED FILE PROCESSORS (CONTINUED)
// ===================================================================

// Enhanced campaign processing - adds period dates to existing detailed sheet
// UPDATED: Campaign processing with append functionality
function processCampaignFileEnhancedAppend(file, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing campaign file (APPEND MODE): ${fileName} (Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}) [Date source: ${periodInfo.date_source}]`);
    
    // Read and process campaign file (same as existing)
    let content;
    try {
      content = file.getBlob().getDataAsString('UTF-16LE');
    } catch (e1) {
      try {
        content = file.getBlob().getDataAsString('UTF-8');
      } catch (e2) {
        try {
          content = file.getBlob().getDataAsString();
        } catch (e3) {
          logWithTime(`Failed to read campaign file: ${e3.message}`, 'ERROR');
          return false;
        }
      }
    }
    
    const rows = parseCSVSafely(content, { delimiter: '\t' });
    if (!rows) return false;
    
    const expectedHeaders = ['Campaign Name', 'Impressions', 'Clicks', 'Total Spent', 'CTR'];
    const headerIdx = findHeaderRowIndex(rows, expectedHeaders);
    const data = convertRowsToObjects(rows, headerIdx);
    
    if (data.length === 0) {
      logWithTime('No campaign data found', 'WARN');
      return false;
    }
    
    // Enhanced data with period boundary dates FROM FILENAME
    const enrichedData = data.map(row => ({
      ...row,
      'Period Start Date': periodInfo.period_start_date,
      'Period End Date': periodInfo.period_end_date,
      'Month': periodInfo.month_name,
      'Year': periodInfo.year,
      'Year_Month': periodInfo.year_month,
      'Quarter': `Q${periodInfo.quarter}`,
      'Days in Period': periodInfo.days_in_period,
      'Report Date': periodInfo.original,
      'Processing Date': formatDate(new Date()),
      'Date Source': periodInfo.date_source,
      'Week': periodInfo.week
    }));
    
    const headers = Object.keys(enrichedData[0]);
    
    // CHANGED: Use append instead of replace
    const success = updateSheetAppend(
      CONFIG.CAMPAIGN_PERFORMANCE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      'Campaign Performance'
    );
    
    if (success) {
      processingState.data.set('campaign_performance', {
        summary: calculateCampaignSummary(enrichedData),
        date: periodInfo,
        period: periodInfo,
        count: enrichedData.length
      });
      logWithTime(`✓ Campaign Performance appended with filename period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Campaign processing error: ${error.message}`, 'ERROR');
    return false;
  }
}
// Enhanced content processing - adds period dates to existing detailed sheet
// UPDATED: Content processing with append functionality
function processContentFileEnhancedAppend(file, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing content file (APPEND MODE): ${fileName} - Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date} [Date source: ${periodInfo.date_source}]`);
    
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const contentHeaders = [
      'post title', 'impressions', 'clicks', 'likes', 'comments', 'reposts',
      'engagement', 'views', 'ctr'
    ];
    
    const headerIdx = findHeaderRowIndex(rows, contentHeaders);
    const data = convertRowsToObjects(rows, headerIdx);
    
    if (data.length === 0) {
      logWithTime('No content data found after conversion', 'WARN');
      return false;
    }
    
    // Enhanced data with period boundary dates FROM FILENAME
    const enrichedData = data.map(row => ({
      ...row,
      'Period Start Date': periodInfo.period_start_date,
      'Period End Date': periodInfo.period_end_date,
      'Month': periodInfo.month_name,
      'Year': periodInfo.year,
      'Year_Month': periodInfo.year_month,
      'Quarter': `Q${periodInfo.quarter}`,
      'Days in Period': periodInfo.days_in_period,
      'Report Date': periodInfo.original,
      'Processing Date': formatDate(new Date()),
      'Date Source': periodInfo.date_source,
      'Week': periodInfo.week
    }));
    
    const headers = Object.keys(enrichedData[0]);
    
    // CHANGED: Use append instead of replace
    const success = updateSheetAppend(
      CONFIG.CONTENT_PERFORMANCE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      'Content Analytics'
    );
    
    if (success) {
      processingState.data.set('content_analytics', {
        summary: calculateContentSummary(enrichedData),
        date: periodInfo,
        period: periodInfo,
        count: enrichedData.length
      });
      logWithTime(`✓ Content Performance appended with filename period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Content processing error: ${error.message}`, 'ERROR');
    return false;
  }
}
// Enhanced processVisitorFile function with proper date handling
// Enhanced visitor processing - adds period dates to existing detailed sheets
function processVisitorFileEnhanced(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing visitor file: ${fileName} (${fileType}) - Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    
    // Read and process file (same as existing)
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime(`No visitor data found in ${fileType}`, 'WARN');
      return false;
    }
    
    // Enhanced data with period dates and date normalization
    const enrichedData = data.map(row => {
      const processedRow = { ...row };
      
      // Date normalization for any Date columns - STANDARDIZE FORMAT
Object.keys(processedRow).forEach(key => {
  const keyLower = key.toLowerCase();
  if (keyLower.includes('date') || keyLower.includes('time')) {
    const originalValue = processedRow[key];
    if (originalValue && typeof originalValue === 'string') {
      processedRow[`Original_${key}`] = originalValue;
      
      try {
        const parsedDate = parseVisitorDate(originalValue);
        if (parsedDate) {
          // FORCE CONSISTENT FORMAT: Always use YYYY-MM-DD
          processedRow[key] = formatDate(parsedDate);
          processedRow[`Normalized_${key}`] = formatDate(parsedDate);
        } else {
          // Manual standardization for unparseable dates
          if (originalValue.includes('/')) {
            const parts = originalValue.split('/');
            if (parts.length === 3) {
              const day = parts[0].padStart(2, '0');
              const month = parts[1].padStart(2, '0');
              const year = parts[2];
              processedRow[key] = `${year}-${month}-${day}`;
            } else {
              processedRow[key] = originalValue;
            }
          } else {
            processedRow[key] = originalValue;
          }
          processedRow[`${key}_Parse_Status`] = 'Manual standardization';
        }
      } catch (dateError) {
        processedRow[key] = originalValue;
        processedRow[`${key}_Parse_Status`] = 'Parse error';
      }
    }
  }
});
      
      // Add period boundary dates
      processedRow['Period Start Date'] = periodInfo.period_start_date;
      processedRow['Period End Date'] = periodInfo.period_end_date;
      processedRow['Month'] = periodInfo.month_name;
      processedRow['Year'] = periodInfo.year;
      processedRow['Year_Month'] = periodInfo.year_month;
      processedRow['Quarter'] = `Q${periodInfo.quarter}`;
      processedRow['Days in Period'] = periodInfo.days_in_period;
      processedRow['Report Date'] = fileDate.original || formatDate(new Date());
      processedRow['Processing Date'] = formatDate(new Date());
      processedRow['File Type'] = fileType;
      processedRow['Week'] = fileDate.week || getCurrentDateInfo().week;
      
      return processedRow;
    });
    
    const sheetId = getVisitorSheetId(fileType);
    if (!sheetId) {
      logWithTime(`No sheet ID found for ${fileType}`, 'ERROR');
      return false;
    }
    
    const headers = Object.keys(enrichedData[0]);
    
    // Update existing detailed visitor sheet with period dates
    const success = updateSheetOptimized(
      sheetId,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      `Visitor: ${fileType}`
    );
    
    if (success && fileType === 'visitor_metrics') {
      processingState.data.set('visitor_metrics', {
        summary: calculateVisitorSummary(enrichedData),
        date: fileDate,
        period: periodInfo,
        count: enrichedData.length
      });
    }
    
    if (success) {
      logWithTime(`✓ ${fileType} updated with period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Visitor processing error (${fileType}): ${error.message}`, 'ERROR');
    return false;
  }
  if (success) {
  // Add sorting call here
  sortSheetChronologically(sheetId, CONFIG.DEFAULT_SHEET_NAME, `Visitor: ${fileType}`);
  
  if (fileType === 'visitor_metrics') {
    processingState.data.set('visitor_metrics', {
      summary: calculateVisitorSummary(enrichedData),
      date: fileDate,
      period: periodInfo,
      count: enrichedData.length
    });
  }
  
  logWithTime(`✓ ${fileType} updated with period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
}
}


// New helper function to parse various date formats in visitor files
function parseVisitorDate(dateString) {
  if (!dateString || typeof dateString !== 'string') return null;
  
  const dateStr = dateString.trim();
  
  // Try different date patterns commonly found in LinkedIn visitor analytics exports
  const patterns = [
    // ISO format: 2025-07-15
    /^(\d{4})-(\d{2})-(\d{2})$/,
    
    // US format: 07/15/2025 or 7/15/2025
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,
    
    // European format: 15/07/2025 or 15.07.2025
    /^(\d{1,2})[\/\.](\d{1,2})[\/\.](\d{4})$/,
    
    // Month name formats: Jul 15, 2025 or July 15, 2025
    /^([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})$/,
    
    // Timestamp format: 2025-07-15 00:00:00
    /^(\d{4})-(\d{2})-(\d{2})\s+\d{2}:\d{2}:\d{2}$/,
    
    // Week-based format: Week of Jul 15, 2025
    /^Week\s+of\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})$/i,
    
    // Range format: Jul 15 - Jul 21, 2025
    /^([A-Za-z]{3,9})\s+(\d{1,2})\s*-\s*[A-Za-z]{3,9}\s+\d{1,2},?\s+(\d{4})$/i
  ];
  
  // Pattern 1: ISO format (2025-07-15)
  let match = dateStr.match(patterns[0]);
  if (match) {
    return new Date(parseInt(match[1]), parseInt(match[2]) - 1, parseInt(match[3]));
  }
  
  // Pattern 2: US format (07/15/2025)
  match = dateStr.match(patterns[1]);
  if (match) {
    return new Date(parseInt(match[3]), parseInt(match[1]) - 1, parseInt(match[2]));
  }
  
  // Pattern 3: European format (15/07/2025)
  match = dateStr.match(patterns[2]);
  if (match) {
    return new Date(parseInt(match[3]), parseInt(match[2]) - 1, parseInt(match[1]));
  }
  
  // Pattern 4: Month name format (Jul 15, 2025)
  match = dateStr.match(patterns[3]);
  if (match) {
    const monthMap = {
      'jan': 0, 'january': 0, 'feb': 1, 'february': 1, 'mar': 2, 'march': 2,
      'apr': 3, 'april': 3, 'may': 4, 'jun': 5, 'june': 5,
      'jul': 6, 'july': 6, 'aug': 7, 'august': 7, 'sep': 8, 'september': 8,
      'oct': 9, 'october': 9, 'nov': 10, 'november': 10, 'dec': 11, 'december': 11
    };
    const month = monthMap[match[1].toLowerCase()];
    if (month !== undefined) {
      return new Date(parseInt(match[3]), month, parseInt(match[2]));
    }
  }
  
  // Pattern 5: Timestamp format (2025-07-15 00:00:00)
  match = dateStr.match(patterns[4]);
  if (match) {
    return new Date(parseInt(match[1]), parseInt(match[2]) - 1, parseInt(match[3]));
  }
  
  // Pattern 6: Week of format (Week of Jul 15, 2025)
  match = dateStr.match(patterns[5]);
  if (match) {
    const monthMap = {
      'jan': 0, 'january': 0, 'feb': 1, 'february': 1, 'mar': 2, 'march': 2,
      'apr': 3, 'april': 3, 'may': 4, 'jun': 5, 'june': 5,
      'jul': 6, 'july': 6, 'aug': 7, 'august': 7, 'sep': 8, 'september': 8,
      'oct': 9, 'october': 9, 'nov': 10, 'november': 10, 'dec': 11, 'december': 11
    };
    const month = monthMap[match[1].toLowerCase()];
    if (month !== undefined) {
      return new Date(parseInt(match[3]), month, parseInt(match[2]));
    }
  }
  
  // Pattern 7: Range format (Jul 15 - Jul 21, 2025) - use start date
  match = dateStr.match(patterns[6]);
  if (match) {
    const monthMap = {
      'jan': 0, 'january': 0, 'feb': 1, 'february': 1, 'mar': 2, 'march': 2,
      'apr': 3, 'april': 3, 'may': 4, 'jun': 5, 'june': 5,
      'jul': 6, 'july': 6, 'aug': 7, 'august': 7, 'sep': 8, 'september': 8,
      'oct': 9, 'october': 9, 'nov': 10, 'november': 10, 'dec': 11, 'december': 11
    };
    const month = monthMap[match[1].toLowerCase()];
    if (month !== undefined) {
      return new Date(parseInt(match[3]), month, parseInt(match[2]));
    }
  }
  
  // Try JavaScript's native Date parsing as last resort
  try {
    const parsed = new Date(dateStr);
    if (!isNaN(parsed.getTime())) {
      return parsed;
    }
  } catch (e) {
    // Fall through to return null
  }
  
  logWithTime(`Could not parse visitor date: "${dateString}"`, 'WARN');
  return null;
}

// Enhanced isLikelyDate function to better detect date formats
function isLikelyDate(value) {
  if (!value || typeof value !== 'string') return false;
  
  const datePatterns = [
    /^\d{4}-\d{2}-\d{2}$/,                    // 2024-12-25
    /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/,  // 2024-12-25 14:30:00
    /^\d{2}\/\d{2}\/\d{4}$/,                  // 12/25/2024
    /^\d{1,2}\/\d{1,2}\/\d{4}$/,              // 1/5/2024
    /^[A-Za-z]{3,9} \d{1,2}, \d{4}$/,         // July 15, 2025
    /^Week of [A-Za-z]{3,9} \d{1,2}, \d{4}$/i, // Week of Jul 15, 2025
    /^[A-Za-z]{3,9} \d{1,2} - [A-Za-z]{3,9} \d{1,2}, \d{4}$/i // Jul 15 - Jul 21, 2025
  ];
  
  return datePatterns.some(pattern => pattern.test(value.trim()));
}

// Updated processing functions with better error handling
// Enhanced processFollowerFile function with proper date handling
// UPDATED: Follower processing with append functionality
// Enhanced data validation for follower processing
// Replace the processFollowerFileEnhancedAppendFixed function with this corrected version
function processFollowerFileEnhancedAppendFixed(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing follower file (HEADERS FIX v2): ${fileName} (${fileType}) - Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime(`No follower data found in ${fileType}`, 'WARN');
      return false;
    }
    
    logWithTime(`Raw data sample: ${JSON.stringify(data[0], null, 2)}`);
    
    // Enhanced data with period dates and date normalization
    const enrichedData = data.map(row => {
      const processedRow = { ...row };
      
      // Date normalization for existing Date columns
      if (processedRow['Date'] || processedRow['date']) {
        const originalDate = processedRow['Date'] || processedRow['date'];
        processedRow['Original_Date'] = originalDate;
        
        try {
          const parsedDate = parseFollowerDate(originalDate);
          if (parsedDate) {
            processedRow['Date'] = formatDate(parsedDate);
            processedRow['Normalized_Date'] = formatDate(parsedDate);
          } else {
            if (originalDate.includes('/')) {
              const parts = originalDate.split('/');
              if (parts.length === 3) {
                const day = parts[0].padStart(2, '0');
                const month = parts[1].padStart(2, '0');
                const year = parts[2];
                processedRow['Date'] = `${year}-${month}-${day}`;
              } else {
                processedRow['Date'] = originalDate;
              }
            } else {
              processedRow['Date'] = originalDate;
            }
            processedRow['Date_Parse_Status'] = 'Manual standardization';
          }
        } catch (dateError) {
          processedRow['Date'] = originalDate;
          processedRow['Date_Parse_Status'] = 'Parse error';
        }
      }
      
      // Add period boundary dates FROM FILENAME
      processedRow['Period Start Date'] = periodInfo.period_start_date;
      processedRow['Period End Date'] = periodInfo.period_end_date;
      processedRow['Month'] = periodInfo.month_name;
      processedRow['Year'] = periodInfo.year;
      processedRow['Year_Month'] = periodInfo.year_month;
      processedRow['Quarter'] = `Q${periodInfo.quarter}`;
      processedRow['Days in Period'] = periodInfo.days_in_period;
      processedRow['Report Date'] = periodInfo.original;
      processedRow['Processing Date'] = formatDate(new Date());
      processedRow['Date Source'] = periodInfo.date_source;
      processedRow['File Type'] = fileType;
      processedRow['Week'] = periodInfo.week;
      
      return processedRow;
    });
    
    logWithTime(`Enriched data sample: ${JSON.stringify(enrichedData[0], null, 2)}`);
    
    // ALWAYS apply demographic processing for follower demographic reports
    let finalData;
    const isLatestPeriod = isCurrentOrMostRecentMonth(periodInfo);
    
    if (['follower_industry', 'follower_company_size', 'follower_seniority', 'follower_function', 'follower_location'].includes(fileType)) {
      logWithTime(`Applying demographic processing for ${fileType}, isLatestPeriod: ${isLatestPeriod}`);
      
      // ALWAYS use the enhanced processing to ensure all headers are present
      finalData = processFollowerDemographicsForLookerStudioFixed(enrichedData, fileType, periodInfo, isLatestPeriod);
      
      logWithTime(`Demographic processing complete. Sample result: ${JSON.stringify(finalData[0], null, 2)}`);
    } else {
      // For follower_metrics, still add the Looker Studio fields for consistency
      finalData = enrichedData.map(row => addLookerStudioFields(row, fileType, periodInfo, isLatestPeriod));
      logWithTime(`Using standard processing with Looker Studio fields for ${fileType}`);
    }
    
    const sheetId = getFollowerSheetId(fileType);
    if (!sheetId) {
      logWithTime(`No sheet ID found for ${fileType}`, 'ERROR');
      return false;
    }
    
    // CRITICAL: Ensure headers are properly extracted and validated
    const headers = Object.keys(finalData[0] || {});
    if (headers.length === 0) {
      logWithTime(`No headers found in final data for ${fileType}`, 'ERROR');
      return false;
    }
    
    // Validate all headers are non-empty strings
    const validHeaders = headers.filter(h => h && h.toString().trim().length > 0);
    if (validHeaders.length !== headers.length) {
      logWithTime(`Warning: ${headers.length - validHeaders.length} empty headers found`, 'WARN');
      logWithTime(`All headers: ${headers.map((h, i) => `${i}: "${h}"`).join(', ')}`);
    }
    
    logWithTime(`Final headers (${validHeaders.length}): ${validHeaders.join(', ')}`);
    
    // Validate each data row has all required fields
    const validatedData = finalData.map((row, index) => {
      const validatedRow = {};
      validHeaders.forEach(header => {
        validatedRow[header] = row[header] !== undefined && row[header] !== null ? row[header] : '';
      });
      return validatedRow;
    });
    
    logWithTime(`Validated data sample: ${JSON.stringify(validatedData[0], null, 2)}`);
    
    // Update sheet with processed data
    const success = updateSheetAppend(
      sheetId,
      CONFIG.DEFAULT_SHEET_NAME,
      validHeaders,
      validatedData,
      `Follower: ${fileType}`
    );
    
    if (success && fileType === 'follower_metrics') {
      processingState.data.set('follower_metrics', {
        summary: calculateFollowerSummary(validatedData),
        date: periodInfo,
        period: periodInfo,
        count: validatedData.length
      });
    }
    
    if (success) {
      logWithTime(`✓ ${fileType} appended successfully: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Follower processing error (${fileType}): ${error.message}`, 'ERROR');
    logWithTime(`Error stack: ${error.stack}`, 'ERROR');
    return false;
  }
}
// ===================================================================
// DIRECT FIX - REPLACE THE MAIN PROCESSING FUNCTION
// ===================================================================

// Clear the sheet and reprocess with correct headers
function clearAndReprocessFollowerFile() {
  try {
    logWithTime('=== Clearing sheet and reprocessing with correct headers ===');
    
    // Clear the follower company size sheet
    const sheet = getOrCreateSheet(CONFIG.FOLLOWER_COMPANY_SIZE_SHEET_ID, CONFIG.DEFAULT_SHEET_NAME);
    sheet.clear();
    logWithTime('Sheet cleared');
    
    // Process the file with the corrected function
    const fileName = "linkedin_follower_company size_aug2025.csv";
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    let targetFile = null;
    while (files.hasNext()) {
      const file = files.next();
      if (file.getName() === fileName) {
        targetFile = file;
        break;
      }
    }
    
    if (!targetFile) {
      logWithTime('File not found');
      return false;
    }
    
    // Process with fixed function
    const result = processFollowerFileDirectFix(targetFile, 'follower_company_size', extractDateFromFileName(fileName));
    
    if (result) {
      checkSheetContents();
    }
    
    return result;
    
  } catch (error) {
    logWithTime(`Clear and reprocess failed: ${error.message}`, 'ERROR');
    return false;
  }
}

// Direct fix processing function with all headers explicitly defined
function processFollowerFileDirectFix(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing with direct fix: ${fileName} (${fileType})`);
    
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime(`No data found in ${fileType}`, 'WARN');
      return false;
    }
    
    logWithTime(`Raw data: ${data.length} rows`);
    logWithTime(`Original columns: ${Object.keys(data[0]).join(', ')}`);
    
    // Calculate totals for percentages
    const totalFollowers = data.reduce((sum, row) => {
      return sum + parseInt(row['Total followers'] || 0);
    }, 0);
    
    logWithTime(`Total followers: ${totalFollowers}`);
    
    const isLatestPeriod = isCurrentOrMostRecentMonth(periodInfo);
    
    // Create fully processed data with ALL required headers
    const processedData = data.map(row => {
      const segmentFollowers = parseInt(row['Total followers'] || 0);
      const segmentPercentage = totalFollowers > 0 ? Math.round((segmentFollowers / totalFollowers) * 10000) / 100 : 0;
      
      return {
        // Original data
        'Company size': row['Company size'] || '',
        'Total followers': segmentFollowers,
        
        // Period information
        'Period Start Date': periodInfo.period_start_date,
        'Period End Date': periodInfo.period_end_date,
        'Month': periodInfo.month_name,
        'Year': periodInfo.year,
        'Year_Month': periodInfo.year_month,
        'Quarter': `Q${periodInfo.quarter}`,
        'Days in Period': periodInfo.days_in_period,
        'Report Date': periodInfo.original,
        'Processing Date': formatDate(new Date()),
        'Date Source': periodInfo.date_source,
        'File Type': fileType,
        'Week': periodInfo.week,
        
        // Looker Studio fields
        'Date_Key': periodInfo.period_start_date,
        'Month_Name': periodInfo.month_name,
        'Is_Latest_Available': isLatestPeriod,
        'Display_Priority': isLatestPeriod ? 1 : 0,
        'Period_Type': 'monthly',
        'Data_Freshness': isLatestPeriod ? 'current' : 'historical',
        
        // Demographic values
        'Segment_Followers': segmentFollowers,
        'Segment_Percentage': segmentPercentage,
        'Total_Follower_Base': totalFollowers,
        'Previous_Month_Total': 0,
        
        // Metadata
        'Report_Type': fileType,
        'Data_Source_File': `follower_company_size_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
        'Upload_Date': formatDate(new Date()),
        'Upload_Time': new Date().toTimeString().split(' ')[0],
        
        // Current and historical fields
        'Current_Total': segmentFollowers,
        'Current_Percentage': segmentPercentage,
        'Historical_Total': segmentFollowers,
        'Historical_Percentage': segmentPercentage,
        
        // Context fields
        'Data_Type': isLatestPeriod ? 'Current Composition' : 'Historical Reference',
        'Display_Context': isLatestPeriod ? 'single_month' : 'range_historical'
      };
    });
    
    logWithTime(`Processed data sample: ${JSON.stringify(processedData[0], null, 2)}`);
    
    // Get headers from the processed data
    const headers = Object.keys(processedData[0]);
    logWithTime(`All headers (${headers.length}): ${headers.join(', ')}`);
    
    // Write to sheet using the standard function
    const success = updateSheetAppend(
      CONFIG.FOLLOWER_COMPANY_SIZE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      processedData,
      `Follower Company Size (Direct Fix)`
    );
    
    if (success) {
      logWithTime(`✓ Successfully processed ${processedData.length} rows with ${headers.length} columns`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Direct fix processing error: ${error.message}`, 'ERROR');
    return false;
  }
}

// Function to verify all headers are populated
function verifyHeaders() {
  try {
    const sheet = getOrCreateSheet(CONFIG.FOLLOWER_COMPANY_SIZE_SHEET_ID, CONFIG.DEFAULT_SHEET_NAME);
    const lastCol = sheet.getLastColumn();
    const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    
    logWithTime(`Current sheet has ${lastCol} columns`);
    
    const emptyHeaders = [];
    headers.forEach((header, index) => {
      if (!header || header.toString().trim() === '') {
        emptyHeaders.push(index + 1);
      }
    });
    
    if (emptyHeaders.length > 0) {
      logWithTime(`Empty headers found at columns: ${emptyHeaders.join(', ')}`);
    } else {
      logWithTime('All headers are populated');
    }
    
    logWithTime(`Headers: ${headers.map((h, i) => `${i+1}:"${h}"`).join(', ')}`);
    
    return { totalColumns: lastCol, emptyHeaders: emptyHeaders.length, headers: headers };
    
  } catch (error) {
    logWithTime(`Header verification failed: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}

logWithTime('Direct fix functions loaded - run clearAndReprocessFollowerFile()');

// NEW FUNCTION: Fixed Looker Studio processing that always adds all fields
function processFollowerDemographicsForLookerStudioFixed(data, reportType, periodInfo, isLatestPeriod) {
  const totalFollowers = calculateTotalFollowersFromData(data);
  
  logWithTime(`Processing demographics: totalFollowers=${totalFollowers}, isLatestPeriod=${isLatestPeriod}`);
  
  return data.map(row => {
    const segmentFollowers = parseInt(row['Total followers'] || row['total followers'] || 0);
    
    // Base row with all original data
    const processedRow = {
      ...row,
      
      // Looker Studio date dimensions (always included)
      'Year_Month': periodInfo.year_month,           // 2025-09
      'Date_Key': periodInfo.period_start_date,      // 2025-09-01 (for Looker date field)
      'Quarter': `Q${periodInfo.quarter}`,           // Q3
      'Month_Name': periodInfo.month_name,           // Sep
      
      // Context metadata for smart filtering
      'Is_Latest_Available': isLatestPeriod,         // TRUE/FALSE for filtering
      'Display_Priority': isLatestPeriod ? 1 : 0,    // For "show latest" filters
      'Period_Type': 'monthly',                      // For multi-period handling
      'Data_Freshness': isLatestPeriod ? 'current' : 'historical',
      
      // Demographic values (always current composition)
      'Segment_Followers': segmentFollowers,
      'Segment_Percentage': totalFollowers > 0 ? Math.round((segmentFollowers / totalFollowers) * 10000) / 100 : 0,
      
      // Reference totals for validation
      'Total_Follower_Base': totalFollowers,
      'Previous_Month_Total': 0, // Could be calculated if needed
      
      // Metadata
      'Report_Type': reportType,
      'Data_Source_File': `follower_${reportType.split('_')[1]}_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
      'Upload_Date': formatDate(new Date()),
      'Upload_Time': new Date().toTimeString().split(' ')[0]
    };
    
    // ALWAYS add both current and historical fields for consistency
    processedRow['Current_Total'] = segmentFollowers;
    processedRow['Current_Percentage'] = processedRow['Segment_Percentage'];
    processedRow['Historical_Total'] = segmentFollowers;
    processedRow['Historical_Percentage'] = processedRow['Segment_Percentage'];
    
    // Add context-specific fields based on whether this is latest data
    if (isLatestPeriod) {
      processedRow['Data_Type'] = 'Current Composition';
      processedRow['Display_Context'] = 'single_month';
    } else {
      processedRow['Data_Type'] = 'Historical Reference';
      processedRow['Display_Context'] = 'range_historical';
    }
    
    return processedRow;
  });
}

// NEW FUNCTION: Add Looker Studio fields to non-demographic data
function addLookerStudioFields(row, reportType, periodInfo, isLatestPeriod) {
  return {
    ...row,
    'Year_Month': periodInfo.year_month,
    'Date_Key': periodInfo.period_start_date,
    'Quarter': `Q${periodInfo.quarter}`,
    'Month_Name': periodInfo.month_name,
    'Is_Latest_Available': isLatestPeriod,
    'Display_Priority': isLatestPeriod ? 1 : 0,
    'Period_Type': 'monthly',
    'Data_Freshness': isLatestPeriod ? 'current' : 'historical',
    'Report_Type': reportType,
    'Data_Source_File': `${reportType}_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
    'Upload_Date': formatDate(new Date()),
    'Upload_Time': new Date().toTimeString().split(' ')[0]
  };
}

// Test function to verify the fix
function testHeadersFix() {
  try {
    logWithTime('=== Testing headers fix ===');
    
    const fileName = "linkedin_follower_company size_aug2025.csv";
    const result = manuallyProcessSpecificFile(fileName);
    
    if (result) {
      // Check the sheet contents again
      checkSheetContents();
    }
    
    return result;
    
  } catch (error) {
    logWithTime(`Headers fix test failed: ${error.message}`, 'ERROR');
    return false;
  }
}

logWithTime('Headers fix v2 loaded - run testHeadersFix() to test');

// NEW FUNCTION: Looker Studio optimized demographic processing
function processFollowerDemographicsForLookerStudio(data, reportType, periodInfo, isLatestPeriod) {
  const totalFollowers = calculateTotalFollowersFromData(data);
  
  return data.map(row => {
    const segmentFollowers = parseInt(row['Total followers'] || row['total followers'] || 0);
    
    // Base row with all original data
    const processedRow = {
      ...row,
      
      // Looker Studio date dimensions (always included)
      Year_Month: periodInfo.year_month,           // 2025-09
      Date_Key: periodInfo.period_start_date,      // 2025-09-01 (for Looker date field)
      Quarter: `Q${periodInfo.quarter}`,           // Q3
      Month_Name: periodInfo.month_name,           // Sep
      
      // Context metadata for smart filtering
      Is_Latest_Available: isLatestPeriod,         // TRUE/FALSE for filtering
      Display_Priority: isLatestPeriod ? 1 : 0,    // For "show latest" filters
      Period_Type: "monthly",                      // For multi-period handling
      Data_Freshness: isLatestPeriod ? "current" : "historical",
      
      // Demographic values (always current composition)
      Segment_Followers: segmentFollowers,
      Segment_Percentage: totalFollowers > 0 ? Math.round((segmentFollowers / totalFollowers) * 10000) / 100 : 0,
      
      // Reference totals for validation
      Total_Follower_Base: totalFollowers,
      Previous_Month_Total: 0, // Could be calculated if needed
      
      // Metadata
      Report_Type: reportType,
      Data_Source_File: `follower_${reportType.split('_')[1]}_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
      Upload_Date: formatDate(new Date()),
      Upload_Time: new Date().toTimeString().split(' ')[0]
    };
    
    // Add context-specific fields based on whether this is latest data
    if (isLatestPeriod) {
      processedRow['Current_Total'] = segmentFollowers;
      processedRow['Current_Percentage'] = processedRow['Segment_Percentage'];
      processedRow['Data_Type'] = 'Current Composition';
      processedRow['Display_Context'] = 'single_month';
    } else {
      processedRow['Historical_Total'] = segmentFollowers;
      processedRow['Historical_Percentage'] = processedRow['Segment_Percentage'];
      processedRow['Data_Type'] = 'Historical Reference';
      processedRow['Display_Context'] = 'range_historical';
    }
    
    return processedRow;
  });
}
// ADD THIS NEW FUNCTION: Enhanced processing for trends
function processFollowerDemographicsTemporal(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const temporalInfo = extractTemporalInfo(fileName);
    
    logWithTime(`Processing follower demographics with temporal tracking: ${fileName} (${fileType})`);
    
    // First, process the file normally
    const success = processFollowerFileEnhancedAppend(file, fileType, fileDate);
    if (!success) return false;
    
    // For trends, we need to get the processed data
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return success;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) return success;
    
    // Check if this is a demographic file type
    if (['follower_industry', 'follower_company_size', 'follower_seniority', 'follower_function', 'follower_location'].includes(fileType)) {
      
      // Enhanced data for trends with full Looker Studio support
      const periodInfo = extractPeriodDatesFromFilename(fileName);
      const isLatest = isCurrentOrMostRecentMonth(periodInfo);
      
      const trendsData = processFollowerDemographicsForLookerStudio(data, fileType, periodInfo, isLatest);
      
      // Update Follower Trends sheet with enhanced structure
      if (ENHANCED_CONFIG.COMPARISON_SHEETS?.FOLLOWER_TRENDS) {
        const headers = Object.keys(trendsData[0]);
        
        const trendsSuccess = updateTrendsSheet(
          ENHANCED_CONFIG.COMPARISON_SHEETS.FOLLOWER_TRENDS,
          CONFIG.DEFAULT_SHEET_NAME,
          headers,
          trendsData,
          'Follower Demographics Trends',
          temporalInfo
        );
        
        if (trendsSuccess) {
          logWithTime(`✓ Updated Follower Demographics Trends: ${trendsData.length} records for ${temporalInfo.month_name} ${temporalInfo.year}`);
        }
      }
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Follower demographics temporal processing error: ${error.message}`, 'ERROR');
    return false;
  }
}
// ENHANCED FUNCTION: Better total followers calculation
function calculateTotalFollowersFromData(data) {
  return data.reduce((sum, row) => {
    // Try multiple possible column names
    const followers = parseInt(row['Total followers'] || row['total followers'] || row['Total Followers'] || 0);
    return sum + followers;
  }, 0);
}
// New helper function to parse various date formats in follower files
function parseFollowerDate(dateString) {
  if (!dateString || typeof dateString !== 'string') return null;
  
  const dateStr = dateString.trim();
  
  // Handle the format change that occurred on July 13th, 2025
  const patterns = [
    // New format from July 13th onwards: MM/DD/YYYY
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,       // 07/13/2025
    
    // Old format before July 13th: YYYY-MM-DD  
    /^(\d{4})-(\d{2})-(\d{2})$/,             // 2025-07-12
    
    // Other possible formats
    /^(\d{4})-(\d{1,2})-(\d{1,2})$/,         // 2025-7-12
    /^(\d{1,2})\/(\d{1,2})\/(\d{2})$/,       // 7/13/25
    
    // European format: DD/MM/YYYY
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,       // Could be 13/07/2025
  ];
  
  // Pattern 1: US format MM/DD/YYYY (from July 13th onwards)
  let match = dateStr.match(patterns[0]);
  if (match) {
    const month = parseInt(match[1]);
    const day = parseInt(match[2]);
    const year = parseInt(match[3]);
    
    // Validate date ranges
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return new Date(year, month - 1, day);
    }
  }
  
  // Pattern 2: ISO format YYYY-MM-DD (before July 13th)
  match = dateStr.match(patterns[1]);
  if (match) {
    const year = parseInt(match[1]);
    const month = parseInt(match[2]);
    const day = parseInt(match[3]);
    
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return new Date(year, month - 1, day);
    }
  }
  
  // Pattern 3: ISO format with single digits YYYY-M-D
  match = dateStr.match(patterns[2]);
  if (match) {
    const year = parseInt(match[1]);
    const month = parseInt(match[2]);
    const day = parseInt(match[3]);
    
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return new Date(year, month - 1, day);
    }
  }
  
  // Try JavaScript's native Date parsing as last resort
  try {
    const parsed = new Date(dateStr);
    if (!isNaN(parsed.getTime())) {
      return parsed;
    }
  } catch (e) {
    // Fall through to return null
  }
  
  logWithTime(`Could not parse follower date: "${dateString}"`, 'WARN');
  return null;
}

// Enhanced fixColumnFormatting to handle follower date columns specifically
function fixColumnFormatting(sheet, headers, allData, reportType) {
  try {
    if (!allData || allData.length === 0) return;
    
    const columnsToFixAsText = [];
    const columnsToFixAsDate = [];
    
    headers.forEach((header, colIndex) => {
      const headerLower = header.toLowerCase();
      
      // Enhanced date column detection for follower files
      if (headerLower.includes('date') || headerLower.includes('time') || 
          headerLower === 'normalized_date' || headerLower === 'original_date') {
        
        // Check if this is actually date data
        let hasValidDates = false;
        for (let rowIndex = 1; rowIndex < Math.min(allData.length, 10); rowIndex++) {
          const cellValue = allData[rowIndex][colIndex];
          if (cellValue && isLikelyDate(cellValue)) {
            hasValidDates = true;
            break;
          }
        }
        
        if (hasValidDates) {
          columnsToFixAsDate.push({
            index: colIndex + 1,
            header: header
          });
          logWithTime(`Will format column ${colIndex + 1} (${header}) as date`);
        }
      }
      // Text formatting for non-date columns
      else if (headerLower.includes('company size') || 
               headerLower.includes('size') ||
               headerLower.includes('range') ||
               headerLower.includes('segment')) {
        
        let hasCompanySizes = false;
        for (let rowIndex = 1; rowIndex < Math.min(allData.length, 20); rowIndex++) {
          const cellValue = allData[rowIndex][colIndex];
          if (cellValue && (isLikelyCompanySize(cellValue) || isLikelyNotDate(cellValue))) {
            hasCompanySizes = true;
            break;
          }
        }
        
        if (hasCompanySizes) {
          columnsToFixAsText.push(colIndex + 1);
          logWithTime(`Will format column ${colIndex + 1} (${header}) as text`);
        }
      }
    });
    
    // Apply text formatting
    columnsToFixAsText.forEach(colNum => {
      try {
        const range = sheet.getRange(1, colNum, allData.length, 1);
        range.setNumberFormat('@');
        logWithTime(`Applied text formatting to column ${colNum}`);
      } catch (formatError) {
        logWithTime(`Failed to format column ${colNum} as text: ${formatError.message}`, 'WARN');
      }
    });
    
    // Apply date formatting with follower-specific logic
    columnsToFixAsDate.forEach(col => {
      try {
        const range = sheet.getRange(2, col.index, allData.length - 1, 1);
        
        // Use appropriate date format for follower data
        let dateFormat = 'yyyy-mm-dd'; // Default format
        
        // Check if this is a specific type of date column
        if (col.header.toLowerCase().includes('processing') || 
            col.header.toLowerCase().includes('report')) {
          dateFormat = 'yyyy-mm-dd hh:mm:ss';
        }
        
        range.setNumberFormat(dateFormat);
        logWithTime(`Applied date formatting (${dateFormat}) to column ${col.index} (${col.header})`);
      } catch (formatError) {
        logWithTime(`Failed to format column ${col.index} as date: ${formatError.message}`, 'WARN');
      }
    });
    
  } catch (error) {
    logWithTime(`Column formatting failed: ${error.message}`, 'WARN');
  }
}

// Enhanced competitor processing - adds period dates to existing detailed sheet
function processCompetitorFileEnhanced(file, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing competitor file: ${fileName} - Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    
    // Read and process file (same as existing)
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime('No competitor data found', 'WARN');
      return false;
    }
    
    // Enhanced data with period boundary dates
    const enrichedData = data.map(row => ({
      ...row,
      'Period Start Date': periodInfo.period_start_date,
      'Period End Date': periodInfo.period_end_date,
      'Month': periodInfo.month_name,
      'Year': periodInfo.year,
      'Year_Month': periodInfo.year_month,
      'Quarter': `Q${periodInfo.quarter}`,
      'Days in Period': periodInfo.days_in_period,
      'Report Date': fileDate.original || formatDate(new Date()),
      'Processing Date': formatDate(new Date()),
      'Week': fileDate.week || getCurrentDateInfo().week
    }));
    
    const headers = Object.keys(enrichedData[0]);
    
    // Update existing detailed competitor sheet with period dates
    const success = updateSheetOptimized(
      CONFIG.COMPETITOR_PERFORMANCE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      enrichedData,
      'Competitor Analytics'
    );
    
    if (success) {
      processingState.data.set('competitor_analytics', {
        summary: calculateCompetitorSummary(enrichedData),
        date: fileDate,
        period: periodInfo,
        count: enrichedData.length
      });
      logWithTime(`✓ Competitor Performance updated with period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Competitor processing error: ${error.message}`, 'ERROR');
    return false;
  }
  if (success) {
  // Add sorting call here
  sortSheetChronologically(CONFIG.COMPETITOR_PERFORMANCE_SHEET_ID, CONFIG.DEFAULT_SHEET_NAME, 'Competitor Analytics');
  
  processingState.data.set('competitor_analytics', {
    summary: calculateCompetitorSummary(enrichedData),
    date: fileDate,
    period: periodInfo,
    count: enrichedData.length
  });
  logWithTime(`✓ Competitor Performance updated with period dates: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
}
}
// ===================================================================
// SHEET ID MAPPING FUNCTIONS
// ===================================================================

function getVisitorSheetId(fileType) {
  const mapping = {
    'visitor_metrics': CONFIG.VISITOR_METRICS_SHEET_ID,
    'visitor_company_size': CONFIG.VISITOR_COMPANY_SIZE_SHEET_ID,
    'visitor_industry': CONFIG.VISITOR_INDUSTRY_SHEET_ID,
    'visitor_seniority': CONFIG.VISITOR_SENIORITY_SHEET_ID,
    'visitor_function': CONFIG.VISITOR_FUNCTION_SHEET_ID,
    'visitor_location': CONFIG.VISITOR_LOCATION_SHEET_ID
  };
  return mapping[fileType];
}

function getFollowerSheetId(fileType) {
  const mapping = {
    'follower_metrics': CONFIG.FOLLOWER_METRICS_SHEET_ID,
    'follower_company_size': CONFIG.FOLLOWER_COMPANY_SIZE_SHEET_ID,
    'follower_industry': CONFIG.FOLLOWER_INDUSTRY_SHEET_ID,
    'follower_seniority': CONFIG.FOLLOWER_SENIORITY_SHEET_ID,
    'follower_function': CONFIG.FOLLOWER_FUNCTION_SHEET_ID,
    'follower_location': CONFIG.FOLLOWER_LOCATION_SHEET_ID
  };
  return mapping[fileType];
}

function getDemographicsSheetId(sectionType) {
  const mapping = {
    'Company Name Segment': CONFIG.DEMOGRAPHICS_COMPANY_SHEET_ID,
    'Company Industry Segment': CONFIG.DEMOGRAPHICS_INDUSTRY_SHEET_ID,
    'Company Size Segment': CONFIG.DEMOGRAPHICS_SIZE_SHEET_ID,
    'Contextual Country/Region Segment': CONFIG.DEMOGRAPHICS_LOCATION_SHEET_ID,  // Countries only
    'Job Seniority Segment': CONFIG.DEMOGRAPHICS_SENIORITY_SHEET_ID,
    'Job Titles Segment': CONFIG.DEMOGRAPHICS_JOB_TITLES_SHEET_ID,
    'Job Title Segment': CONFIG.DEMOGRAPHICS_JOB_TITLES_SHEET_ID,   
    'Job Function Segment': CONFIG.DEMOGRAPHICS_FUNCTION_SHEET_ID,
    'Industry Segment': CONFIG.DEMOGRAPHICS_INDUSTRY_SHEET_ID,
    'Seniority Segment': CONFIG.DEMOGRAPHICS_SENIORITY_SHEET_ID,
    'Function Segment': CONFIG.DEMOGRAPHICS_FUNCTION_SHEET_ID,
    
    // Route city-level data to overview instead of location sheet
    'Location Segment': CONFIG.DEMOGRAPHICS_OVERVIEW_SHEET_ID,  // Cities go to overview
    'County Segment': CONFIG.DEMOGRAPHICS_OVERVIEW_SHEET_ID     // Counties go to overview
  };
  return mapping[sectionType] || CONFIG.DEMOGRAPHICS_OVERVIEW_SHEET_ID;
}

// ===================================================================
// SUMMARY CALCULATIONS
// ===================================================================

function calculateCampaignSummary(data) {
  try {
    const summary = {
      total_campaigns: data.length,
      total_spend: 0,
      total_impressions: 0,
      total_clicks: 0,
      avg_ctr: 0,
      avg_cpc: 0
    };
    
    data.forEach(row => {
      const spend = parseFloat(row['Total Spent'] || row['total_spent'] || 0);
      const impressions = parseInt(row['Impressions'] || row['impressions'] || 0);
      const clicks = parseInt(row['Clicks'] || row['clicks'] || 0);
      
      summary.total_spend += spend;
      summary.total_impressions += impressions;
      summary.total_clicks += clicks;
    });
    
    if (summary.total_impressions > 0) {
      summary.avg_ctr = (summary.total_clicks / summary.total_impressions * 100);
    }
    
    if (summary.total_clicks > 0) {
      summary.avg_cpc = summary.total_spend / summary.total_clicks;
    }
    
    // Round to reasonable precision
    summary.avg_ctr = Math.round(summary.avg_ctr * 100) / 100;
    summary.avg_cpc = Math.round(summary.avg_cpc * 100) / 100;
    
    return summary;
    
  } catch (error) {
    logWithTime(`Campaign summary calculation error: ${error.message}`, 'ERROR');
    return { total_campaigns: data?.length || 0 };
  }
}

function calculateContentSummary(data) {
  try {
    const summary = {
      total_posts: data.length,
      total_impressions: 0,
      total_engagement: 0,
      avg_engagement_rate: 0,
      top_performing_post: null
    };
    
    let maxEngagement = 0;
    
    data.forEach(row => {
      const impressions = parseInt(row['Impressions'] || row['impressions'] || 0);
      const likes = parseInt(row['Likes'] || row['likes'] || 0);
      const comments = parseInt(row['Comments'] || row['comments'] || 0);
      const shares = parseInt(row['Shares'] || row['reposts'] || row['shares'] || 0);
      
      const totalEngagement = likes + comments + shares;
      
      summary.total_impressions += impressions;
      summary.total_engagement += totalEngagement;
      
      if (totalEngagement > maxEngagement) {
        maxEngagement = totalEngagement;
        summary.top_performing_post = row['Post Title'] || row['post_title'] || 'Unknown';
      }
    });
    
    if (summary.total_impressions > 0) {
      summary.avg_engagement_rate = (summary.total_engagement / summary.total_impressions * 100);
      summary.avg_engagement_rate = Math.round(summary.avg_engagement_rate * 100) / 100;
    }
    
    return summary;
    
  } catch (error) {
    logWithTime(`Content summary calculation error: ${error.message}`, 'ERROR');
    return { total_posts: data?.length || 0 };
  }
}

function calculateFollowerSummary(data) {
  try {
    const summary = {
      total_new_followers: 0,
      organic_followers: 0,
      sponsored_followers: 0,
      growth_rate: 0
    };
    
    data.forEach(row => {
      summary.total_new_followers += parseInt(row['Total followers'] || row['new_followers'] || 0);
      summary.organic_followers += parseInt(row['Organic followers'] || row['organic_followers'] || 0);
      summary.sponsored_followers += parseInt(row['Sponsored followers'] || row['sponsored_followers'] || 0);
    });
    
    const totalFollowers = parseInt(data[0]?.['Total followers'] || 0);
    if (totalFollowers > 0 && summary.total_new_followers > 0) {
      summary.growth_rate = (summary.total_new_followers / totalFollowers * 100);
      summary.growth_rate = Math.round(summary.growth_rate * 100) / 100;
    }
    
    return summary;
    
  } catch (error) {
    logWithTime(`Follower summary calculation error: ${error.message}`, 'ERROR');
    return { total_new_followers: 0 };
  }
}

function calculateVisitorSummary(data) {
  try {
    const summary = {
      total_visitors: 0,
      unique_visitors: 0,
      page_views: 0
    };
    
    data.forEach(row => {
      summary.total_visitors += parseInt(row['Total visitors'] || row['visitors'] || 0);
      summary.unique_visitors += parseInt(row['Unique visitors'] || row['unique_visitors'] || 0);
      summary.page_views += parseInt(row['Page views'] || row['page_views'] || 0);
    });
    
    return summary;
    
  } catch (error) {
    logWithTime(`Visitor summary calculation error: ${error.message}`, 'ERROR');
    return { total_visitors: 0 };
  }
}

function calculateCompetitorSummary(data) {
  try {
    return {
      total_competitors: data.length,
      analyzed_date: formatDate(new Date()),
      data_points: data.reduce((sum, row) => {
        return sum + Object.keys(row).length;
      }, 0)
    };
  } catch (error) {
    logWithTime(`Competitor summary calculation error: ${error.message}`, 'ERROR');
    return { total_competitors: data?.length || 0 };
  }
}

// ===================================================================
// MASTER DASHBOARD UPDATE
// ===================================================================

function updateMasterDashboard(processedFiles) {
  try {
    logWithTime('Updating master dashboard');
    
    const sheet = getOrCreateSheet(CONFIG.MASTER_DASHBOARD_SHEET_ID, CONFIG.DEFAULT_SHEET_NAME);
    
    const campaignData = processingState.data.get('campaign_performance')?.summary || {};
    const contentData = processingState.data.get('content_analytics')?.summary || {};
    const followerData = processingState.data.get('follower_metrics')?.summary || {};
    const visitorData = processingState.data.get('visitor_metrics')?.summary || {};
    const competitorData = processingState.data.get('competitor_analytics')?.summary || {};
    const demographicsData = processingState.data.get('demographics')?.summary || {};
    
    const processingTime = processingState.startTime ? 
      ((Date.now() - processingState.startTime) / 1000).toFixed(1) : '0';
    
    const dashboardData = [
      ['LinkedIn Analytics Dashboard - WITH JOB TITLES', '', formatDate(new Date())],
      ['Last Updated', formatDate(new Date()), `${processingTime}s processing time`],
      ['', '', ''],
      ['FILES PROCESSED', 'Count', 'Status'],
      ['Campaign Files', processedFiles.campaign || 0, processedFiles.campaign > 0 ? '✓' : '✗'],
      ['Content Files', processedFiles.content || 0, processedFiles.content > 0 ? '✓' : '✗'],
      ['Demographics Files (incl. Job Titles)', processedFiles.demographics || 0, processedFiles.demographics > 0 ? '✓' : '✗'],
      ['Visitor Files', processedFiles.visitor || 0, processedFiles.visitor > 0 ? '✓' : '✗'],
      ['Follower Files', processedFiles.follower || 0, processedFiles.follower > 0 ? '✓' : '✗'],
      ['Competitor Files', processedFiles.competitor || 0, processedFiles.competitor > 0 ? '✓' : '✗'],
      ['Errors', processedFiles.errors || 0, processedFiles.errors === 0 ? '✓' : '⚠️'],
      ['Skipped Files', processedFiles.skipped || 0, ''],
      ['', '', ''],
      ['KEY PERFORMANCE METRICS', 'Value', 'Trend'],
      ['Total Campaigns', campaignData.total_campaigns || 0, ''],
      ['Campaign Spend', `${(campaignData.total_spend || 0).toLocaleString()}`, ''],
      ['Average CTR', `${campaignData.avg_ctr || 0}%`, ''],
      ['Average CPC', `${campaignData.avg_cpc || 0}`, ''],
      ['Total Posts', contentData.total_posts || 0, ''],
      ['Avg Engagement Rate', `${contentData.avg_engagement_rate || 0}%`, ''],
      ['New Followers', followerData.total_new_followers || 0, ''],
      ['Follower Growth Rate', `${followerData.growth_rate || 0}%`, ''],
      ['Total Visitors', visitorData.total_visitors || 0, ''],
      ['Competitors Tracked', competitorData.total_competitors || 0, ''],
      ['Demographics Sections', demographicsData.sections_processed || 0, ''],
      ['', '', ''],
      ['DATA QUALITY CHECKS', 'Status', 'Notes'],
      ['Campaign Data', campaignData.total_campaigns > 0 ? 'Good' : 'Missing', ''],
      ['Content Data', contentData.total_posts > 0 ? 'Good' : 'Missing', ''],
      ['Audience Data', followerData.total_new_followers >= 0 ? 'Good' : 'Missing', ''],
      ['Job Titles Support', 'ENABLED', 'Ready to process Job Title segments'],
      ['Processing Errors', processedFiles.errors === 0 ? 'None' : `${processedFiles.errors} errors`, '']
    ];
    
    // Clear and update sheet
    sheet.clear();
    sheet.getRange(1, 1, dashboardData.length, 3).setValues(dashboardData);
    
    // Enhanced formatting
    formatDashboard(sheet, dashboardData.length);
    
    logWithTime('✓ Master dashboard updated successfully');
    return true;
    
  } catch (error) {
    logWithTime(`Dashboard update error: ${error.message}`, 'ERROR');
    return false;
  }
}

function formatDashboard(sheet, rowCount) {
  try {
    // Title formatting
    sheet.getRange(1, 1, 1, 3)
      .merge()
      .setFontSize(18)
      .setFontWeight('bold')
      .setBackground('#0066cc')
      .setFontColor('white')
      .setHorizontalAlignment('center');
    
    // Section headers
    const sectionHeaders = [4, 14, 26]; // Row numbers for section headers
    sectionHeaders.forEach(row => {
      if (row <= rowCount) {
        sheet.getRange(row, 1, 1, 3)
          .setFontWeight('bold')
          .setBackground('#e6f3ff')
          .setFontColor('#0066cc');
      }
    });
    
    // Auto-resize columns
    sheet.autoResizeColumns(1, 3);
    
    // Add alternating colors for data sections
    for (let i = 5; i <= 12; i++) { // Files processed section
      if (i % 2 === 0) {
        sheet.getRange(i, 1, 1, 3).setBackground('#f8f9fa');
      }
    }
    
    for (let i = 15; i <= 25; i++) { // Metrics section
      if (i % 2 === 0) {
        sheet.getRange(i, 1, 1, 3).setBackground('#f8f9fa');
      }
    }
    
  } catch (error) {
    logWithTime(`Dashboard formatting failed: ${error.message}`, 'WARN');
  }
}
// ===================================================================
// MAIN PROCESSING FUNCTION
// ===================================================================

function processAllLinkedInAnalytics() {
  processingState.startTime = Date.now(); // Fix: Use Date.now() for milliseconds
  clearProcessingState();
  
  try {
    logWithTime('=== Starting LinkedIn Analytics Processing ===');
    
    // Validate folders first
    const folders = validateFolders();
    if (!folders) {
      throw new Error('Folder validation failed');
    }
    
    const { incomingFolder, processedFolder } = folders;
    
    // Get files safely
    const files = getFilesFromFolder(incomingFolder);
    logWithTime(`Found ${files.length} files to process`);
    
    if (files.length === 0) {
      logWithTime('No files to process');
      return { success: true, processed: 0, message: 'No files found' };
    }
    
    if (files.length > CONFIG.MAX_FILES_PER_BATCH) {
      logWithTime(`Large batch warning: ${files.length} files (max: ${CONFIG.MAX_FILES_PER_BATCH})`, 'WARN');
    }
    
    // Initialize results tracking
    const results = {
      campaign: 0,
      content: 0,
      visitor: 0,
      follower: 0,
      competitor: 0,
      demographics: 0,
      errors: 0,
      skipped: 0
    };
    
    const errorDetails = [];
    
    // Process each file
    for (let i = 0; i < files.length; i++) {
      // Check timeout properly
      const elapsedTime = Date.now() - processingState.startTime;
      if (elapsedTime > CONFIG.MAX_PROCESSING_TIME_MS) {
        logWithTime(`Processing timeout reached after ${(elapsedTime / 1000).toFixed(1)}s, stopping gracefully`, 'WARN');
        break;
      }
      
      logWithTime(`Processing file ${i + 1}/${files.length} - elapsed: ${(elapsedTime / 1000).toFixed(1)}s`);
      
      try {
        const file = files[i];
        
        // Validate file object exists
        if (!file) {
          logWithTime(`File at index ${i} is null or undefined`, 'ERROR');
          results.errors++;
          continue;
        }
        
        let fileName;
        try {
          fileName = file.getName();
        } catch (nameError) {
          logWithTime(`Cannot get filename for file at index ${i}: ${nameError.message}`, 'ERROR');
          results.errors++;
          continue;
        }
        
        if (!fileName) {
          logWithTime(`Empty filename for file at index ${i}`, 'ERROR');
          results.errors++;
          continue;
        }
        
        logWithTime(`[${i + 1}/${files.length}] Processing: ${fileName}`);
        
        // Validate file first
        const fileValidation = validateFile(file);
        if (!fileValidation.valid) {
          logWithTime(`Skipping invalid file: ${fileValidation.error}`, 'WARN');
          results.skipped++;
          continue;
        }
        
        // Check if it's a LinkedIn file
        if (!isLinkedInFile(fileName)) {
          logWithTime(`Skipping non-LinkedIn file: ${fileName}`);
          results.skipped++;
          continue;
        }
        
        const fileType = detectFileType(fileName);
        const fileDate = extractDateFromFileName(fileName);
        
        logWithTime(`File type: ${fileType}, Date: ${JSON.stringify(fileDate)}`);
        
        let success = false;
        
        // Route to appropriate processor with additional file validation
        try {
          // Re-validate file before each processor call
          const preProcessValidation = validateFileObject(file, `pre-${fileType}`);
          if (!preProcessValidation.valid) {
            logWithTime(`File became invalid before ${fileType} processing: ${preProcessValidation.error}`, 'ERROR');
            results.errors++;
            errorDetails.push({ file: fileName, type: fileType, error: preProcessValidation.error });
            continue;
          }
          
          switch (fileType) {
            case 'campaign_performance':
              logWithTime(`Routing to campaign processor: ${fileName}`);
              success = processCampaignFile(file, fileDate);
              if (success) results.campaign++;
              break;
              
            case 'demographics_report':
              logWithTime(`Routing to demographics processor: ${fileName}`);
              success = processDemographicsFile(file, fileDate);
              if (success) results.demographics++;
              break;
              
            case 'content_analytics':
              logWithTime(`Routing to content processor: ${fileName}`);
              success = processContentFile(file, fileDate);
              if (success) results.content++;
              break;
              
            case 'visitor_metrics':
            case 'visitor_company_size':
            case 'visitor_industry':
            case 'visitor_seniority':
            case 'visitor_function':
            case 'visitor_location':
              logWithTime(`Routing to visitor processor (${fileType}): ${fileName}`);
              success = processVisitorFile(file, fileType, fileDate);
              if (success) results.visitor++;
              break;
              
            case 'follower_metrics':
            case 'follower_company_size':
            case 'follower_industry':
            case 'follower_seniority':
            case 'follower_function':
            case 'follower_location':
              logWithTime(`Routing to follower processor (${fileType}): ${fileName}`);
              success = processFollowerFile(file, fileType, fileDate);
              if (success) results.follower++;
              break;
              
            case 'competitor_analytics':
              logWithTime(`Routing to competitor processor: ${fileName}`);
              success = processCompetitorFile(file, fileDate);
              if (success) results.competitor++;
              break;
              
            default:
              logWithTime(`Unknown file type: ${fileType} for ${fileName}`, 'WARN');
              results.skipped++;
              continue;
          }
        } catch (processingError) {
          logWithTime(`Processing function failed for ${fileName}: ${processingError.message}`, 'ERROR');
          logWithTime(`Stack trace: ${processingError.stack}`, 'ERROR');
          success = false;
        }
        
        // Handle file movement
        if (success) {
          try {
            // Verify file still exists before moving
            file.getName(); // This will throw if file is invalid
            file.moveTo(processedFolder);
            processingState.processedCount++;
            logWithTime(`✓ Successfully processed and moved: ${fileName}`);
          } catch (moveError) {
            logWithTime(`File processed but move failed: ${moveError.message}`, 'WARN');
            processingState.processedCount++;
          }
        } else {
          results.errors++;
          processingState.errorCount++;
          errorDetails.push({ file: fileName, type: fileType, error: 'Processing failed' });
          logWithTime(`✗ Failed to process: ${fileName}`, 'ERROR');
        }
        
        // Memory management
        if ((i + 1) % CONFIG.MEMORY_CLEANUP_INTERVAL === 0) {
          checkMemoryUsage();
          Utilities.sleep(200); // Brief pause for memory cleanup
        }
        
      } catch (fileError) {
        results.errors++;
        processingState.errorCount++;
        const fileName = files[i]?.getName() || 'Unknown';
        errorDetails.push({ file: fileName, error: fileError.message });
        logWithTime(`File processing exception: ${fileError.message}`, 'ERROR');
        continue;
      }
    }
    
    // Generate final reports
    const processingEndTime = Date.now();
    const processingTime = (processingEndTime - processingState.startTime) / 1000; // Convert to seconds
    const totalProcessed = processingState.processedCount;
    
    logWithTime(`=== Processing Summary ===`);
    logWithTime(`Start time: ${processingState.startTime}, End time: ${processingEndTime}`);
    logWithTime(`Time: ${processingTime.toFixed(1)}s`);
    logWithTime(`Processed: ${totalProcessed} files`);
    logWithTime(`Errors: ${processingState.errorCount} files`);
    logWithTime(`Success Rate: ${totalProcessed > 0 ? ((totalProcessed / (totalProcessed + processingState.errorCount)) * 100).toFixed(1) : 0}%`);
    
    // Update dashboard and send notifications
    try {
      updateMasterDashboard(results);
      sendProcessingSummary(results, processingTime, errorDetails);
    } catch (reportError) {
      logWithTime(`Report generation failed: ${reportError.message}`, 'ERROR');
    }
    
    // Cleanup
    clearProcessingState();
    
    const finalResult = {
      success: true,
      processed: totalProcessed,
      errors: processingState.errorCount,
      time: processingTime,
      details: results
    };
    
    if (errorDetails.length > 0) {
      finalResult.errorDetails = errorDetails;
    }
    
    return finalResult;
    
  } catch (error) {
    logWithTime(`Critical processing error: ${error.message}`, 'ERROR');
    
    try {
      sendErrorNotification(error);
    } catch (notificationError) {
      logWithTime(`Error notification failed: ${notificationError.message}`, 'ERROR');
    }
    
    clearProcessingState();
    
    return {
      success: false,
      error: error.message,
      processed: processingState.processedCount,
      time: processingState.startTime ? (Date.now() - processingState.startTime) / 1000 : 0
    };
  }
}
// Main temporal processing function
function processAllLinkedInAnalyticsTemporal() {
  try {
    processingState.startTime = Date.now();
    clearProcessingState();
    
    logWithTime('=== Starting Temporal LinkedIn Analytics Processing ===');
    
    const folders = validateFolders();
    if (!folders) throw new Error('Folder validation failed');
    
    const files = getFilesFromFolder(folders.incomingFolder);
    logWithTime(`Found ${files.length} files for temporal processing`);
    
    if (files.length === 0) {
      return { success: true, processed: 0, message: 'No files found' };
    }
    
    const results = {
      campaign: 0, content: 0, visitor: 0, follower: 0,
      competitor: 0, demographics: 0, errors: 0, skipped: 0,
      months_processed: new Set()
    };
    
    // Process each file with temporal tracking
    for (let i = 0; i < files.length; i++) {
      try {
        const file = files[i];
        const fileName = file.getName();
        
        if (!isLinkedInFile(fileName)) {
          results.skipped++;
          continue;
        }
        
        const fileType = detectFileType(fileName);
        const temporalInfo = extractTemporalInfo(fileName);
        results.months_processed.add(temporalInfo.year_month);
        
        logWithTime(`[${i + 1}/${files.length}] Processing: ${fileName} (${temporalInfo.month_name} ${temporalInfo.year})`);
        
        let success = false;
        
        switch (fileType) {
          case 'campaign_performance':
            success = processCampaignFileTemporal(file, extractDateFromFileName(fileName));
            if (success) results.campaign++;
            break;
            
          case 'demographics_report':
            success = processDemographicsFileTemporal(file, extractDateFromFileName(fileName));
            if (success) results.demographics++;
            break;
            
          // Add other file types following the same temporal pattern
        }
        
        if (success) {
          file.moveTo(folders.processedFolder);
        } else {
          results.errors++;
        }
        
      } catch (error) {
        results.errors++;
        logWithTime(`Temporal processing error: ${error.message}`, 'ERROR');
      }
    }
    
    // Update comparison dashboard
    updateMonthlyComparisonDashboard();
    
    const processingTime = (Date.now() - processingState.startTime) / 1000;
    const totalProcessed = results.campaign + results.content + results.visitor + 
                          results.follower + results.demographics + results.competitor;
    
    logWithTime(`=== Temporal Processing Complete ===`);
    logWithTime(`Processed: ${totalProcessed} files across ${results.months_processed.size} months in ${processingTime.toFixed(1)}s`);
    logWithTime(`Months: ${Array.from(results.months_processed).join(', ')}`);
    
    return {
      success: true,
      processed: totalProcessed,
      errors: results.errors,
      time: processingTime,
      months: Array.from(results.months_processed),
      details: results
    };
    
  } catch (error) {
    logWithTime(`Temporal processing failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

function processAllLinkedInReports() {
  try {
    // Fix: Initialize timing properly
    const actualStartTime = Date.now();
    logWithTime('=== Starting LinkedIn Analytics Processing with Job Titles Support ===');
    
    // Clear state but preserve start time
    processingState.data.clear();
    processingState.processedCount = 0;
    processingState.errorCount = 0;
    processingState.memoryUsage = 0;
    processingState.startTime = actualStartTime; // Set this AFTER clearing
    
    const folders = validateFolders();
    if (!folders) {
      throw new Error('Folder validation failed');
    }
    
    const { incomingFolder, processedFolder } = folders;
    const files = getFilesFromFolder(incomingFolder);
    logWithTime(`Found ${files.length} files to process`);
    
    if (files.length === 0) {
      return { success: true, processed: 0, message: 'No files found' };
    }
    
    const results = {
      campaign: 0, content: 0, visitor: 0, follower: 0,
      competitor: 0, demographics: 0, errors: 0, skipped: 0
    };
    
    const errorDetails = [];
    
    // Process each file with proper timeout checking
    for (let i = 0; i < files.length; i++) {
      const elapsedTime = Date.now() - actualStartTime; // Use actualStartTime
      
      if (elapsedTime > CONFIG.MAX_PROCESSING_TIME_MS) {
        logWithTime(`Processing timeout reached after ${(elapsedTime / 1000).toFixed(1)}s, stopping gracefully`, 'WARN');
        break;
      }
      
      logWithTime(`Processing file ${i + 1}/${files.length} - elapsed: ${(elapsedTime / 1000).toFixed(1)}s`);
      
      try {
        const file = files[i];
        if (!file) continue;
        
        const fileName = file.getName();
        logWithTime(`[${i + 1}/${files.length}] Processing: ${fileName}`);
        
        const fileValidation = validateFile(file);
        if (!fileValidation.valid) {
          logWithTime(`Skipping invalid file: ${fileValidation.error}`, 'WARN');
          results.skipped++;
          continue;
        }
        
        if (!isLinkedInFile(fileName)) {
          logWithTime(`Skipping non-LinkedIn file: ${fileName}`);
          results.skipped++;
          continue;
        }
        
        const fileType = detectFileType(fileName);
        const fileDate = extractDateFromFileName(fileName);
        
        logWithTime(`File type: ${fileType}, Date: ${JSON.stringify(fileDate)}`);
        
        let success = false;
        
        // Route to appropriate processor
        switch (fileType) {
          case 'demographics_report':
            logWithTime(`*** PROCESSING DEMOGRAPHICS FILE (with Job Titles support) ***`);
            success = processDemographicsFile(file, fileDate);
            if (success) results.demographics++;
            break;
            
          case 'campaign_performance':
            success = processCampaignFile(file, fileDate);
            if (success) results.campaign++;
            break;
            
          case 'content_analytics':
            success = processContentFile(file, fileDate);
            if (success) results.content++;
            break;
            
          // Add other cases as needed
          
          default:
            logWithTime(`Unknown file type: ${fileType} for ${fileName}`, 'WARN');
            results.skipped++;
            continue;
        }
        
        if (success) {
          try {
            file.moveTo(processedFolder);
            processingState.processedCount++;
            logWithTime(`✓ Successfully processed and moved: ${fileName}`);
          } catch (moveError) {
            logWithTime(`File processed but move failed: ${moveError.message}`, 'WARN');
            processingState.processedCount++;
          }
        } else {
          results.errors++;
          processingState.errorCount++;
          errorDetails.push({ file: fileName, type: fileType, error: 'Processing failed' });
          logWithTime(`✗ Failed to process: ${fileName}`, 'ERROR');
        }
        
      } catch (fileError) {
        results.errors++;
        processingState.errorCount++;
        logWithTime(`File processing exception: ${fileError.message}`, 'ERROR');
        continue;
      }
    }
    
    const processingTime = (Date.now() - actualStartTime) / 1000; // Use actualStartTime
    
    logWithTime(`=== Processing Summary ===`);
    logWithTime(`Time: ${processingTime.toFixed(1)}s`);
    logWithTime(`Processed: ${processingState.processedCount} files`);
    logWithTime(`Errors: ${processingState.errorCount} files`);
    
    try {
      updateMasterDashboard(results);
      sendProcessingSummary(results, processingTime, errorDetails);
    } catch (reportError) {
      logWithTime(`Report generation failed: ${reportError.message}`, 'ERROR');
    }
    
    return {
      success: true,
      processed: processingState.processedCount,
      errors: processingState.errorCount,
      time: processingTime,
      details: results
    };
    
  } catch (error) {
    logWithTime(`Critical processing error: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

// ===================================================================
// HELPER FUNCTIONS
// ===================================================================

function getFilesFromFolder(folder) {
  const files = [];
  
  try {
    logWithTime('Getting files from folder...');
    
    const fileIterator = folder.getFiles();
    let fileCount = 0;
    
    while (fileIterator.hasNext() && fileCount < CONFIG.MAX_FILES_PER_BATCH) {
      try {
        const file = fileIterator.next();
        
        // Validate file object before adding to array
        if (file) {
          // Test if file is accessible
          const fileName = file.getName();
          if (fileName && fileName.length > 0) {
            files.push(file);
            fileCount++;
            logWithTime(`Found file: ${fileName}`);
          } else {
            logWithTime('Skipped file with empty name', 'WARN');
          }
        } else {
          logWithTime('Skipped null file from iterator', 'WARN');
        }
        
      } catch (fileError) {
        logWithTime(`Error accessing file in iteration: ${fileError.message}`, 'WARN');
        continue; // Skip this file and continue with others
      }
    }
    
    logWithTime(`Successfully retrieved ${files.length} files from folder`);
    return files;
    
  } catch (error) {
    logWithTime(`Error getting files from folder: ${error.message}`, 'ERROR');
    return [];
  }
}

function validateFolders() {
  try {
    logWithTime('Validating folder access...');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
    
    // Test access
    incomingFolder.getName();
    processedFolder.getName();
    
    logWithTime('✓ Folder validation successful');
    return { incomingFolder, processedFolder };
    
  } catch (error) {
    logWithTime(`✗ Folder validation failed: ${error.message}`, 'ERROR');
    return null;
  }
}

// ===================================================================
// NOTIFICATION SYSTEM
// ===================================================================

function sendProcessingSummary(results, processingTime, errorDetails = []) {
  try {
    const totalProcessed = Object.values(results).reduce((sum, count) => {
      return typeof count === 'number' ? sum + count : sum;
    }, 0) - results.errors - results.skipped;
    
    const subject = totalProcessed > 0 ? 
      `✅ LinkedIn Analytics Complete - ${totalProcessed} files processed` :
      `⚠️ LinkedIn Analytics Complete - No files processed`;
    
    const campaignSummary = processingState.data.get('campaign_performance')?.summary || {};
    const contentSummary = processingState.data.get('content_analytics')?.summary || {};
    const followerSummary = processingState.data.get('follower_metrics')?.summary || {};
    
    let body = `LinkedIn Analytics Processing Complete!
========================================

📊 PROCESSING SUMMARY:
✅ Total Files Processed: ${totalProcessed}
📈 Campaign Files: ${results.campaign}
📝 Content Files: ${results.content}
📊 Demographics Files (with Job Titles): ${results.demographics}
👥 Visitor Files: ${results.visitor}
🔔 Follower Files: ${results.follower}
🏢 Competitor Files: ${results.competitor}
❌ Errors: ${results.errors}
⏭️ Skipped: ${results.skipped}
⏱️ Processing Time: ${processingTime.toFixed(1)} seconds

🎯 KEY INSIGHTS:
- Campaigns: ${campaignSummary.total_campaigns || 0} analyzed, ${(campaignSummary.total_spend || 0).toLocaleString()} spend, ${campaignSummary.avg_ctr || 0}% CTR
- Content: ${contentSummary.total_posts || 0} posts, ${contentSummary.avg_engagement_rate || 0}% avg engagement rate
- Followers: ${followerSummary.total_new_followers || 0} new followers, ${followerSummary.growth_rate || 0}% growth rate

✨ JOB TITLES SUPPORT: Job Title demographics are now automatically processed!

📈 All dashboards have been updated with the latest data.
🕐 Processing completed at: ${formatDate(new Date())} ${new Date().toLocaleTimeString()}`;

    if (errorDetails.length > 0) {
      body += `\n\n⚠️ ERRORS ENCOUNTERED:\n`;
      errorDetails.forEach(error => {
        body += `• ${error.file}: ${error.error}\n`;
      });
      body += `\nPlease check the files and try reprocessing them.`;
    }

    body += `\n\n🔗 View your updated dashboards in Looker Studio
📧 This is an automated report from LinkedIn Analytics Automation`;
    
    GmailApp.sendEmail(
      Session.getActiveUser().getEmail(),
      subject,
      body
    );
    
    logWithTime('✅ Summary email sent successfully');
    
  } catch (error) {
    logWithTime(`Email notification failed: ${error.message}`, 'ERROR');
  }
}

function sendErrorNotification(error) {
  try {
    const subject = '🚨 LinkedIn Analytics Processing Error';
    const body = `LinkedIn Analytics Error Report
====================================

💥 Error: ${error.message}
🕐 Time: ${formatDate(new Date())} ${new Date().toLocaleTimeString()}
📁 Files Processed Before Error: ${processingState.processedCount}
⚠️ Total Errors: ${processingState.errorCount}

📋 Stack Trace:
${error.stack || 'No stack trace available'}

🔧 TROUBLESHOOTING STEPS:
1. Check if all folder IDs in CONFIG are correct
2. Verify file formats match LinkedIn export standards
3. Ensure all Google Sheets are accessible (including Job Titles sheet)
4. Check Google Apps Script execution logs for details

📧 This is an automated error report from LinkedIn Analytics Automation.
Please review the settings and try running the process again.`;
    
    GmailApp.sendEmail(
      Session.getActiveUser().getEmail(),
      subject,
      body
    );
    
    logWithTime('Error notification sent');
    
  } catch (emailError) {
    logWithTime(`Error notification failed: ${emailError.message}`, 'ERROR');
  }
}

function setupLinkedInAutomation() {
  logWithTime('🚀 Setting up LinkedIn Analytics automation with Job Titles support...');
  
  try {
    // Remove existing triggers to prevent duplicates
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === 'processAllLinkedInAnalytics') {
        ScriptApp.deleteTrigger(trigger);
        logWithTime('Removed existing trigger');
      }
    });
    
    // Create new trigger - runs every 6 hours
    ScriptApp.newTrigger('processAllLinkedInAnalytics')
             .timeBased()
             .everyHours(6)
             .create();
    
    logWithTime('✅ Automation trigger created - will run every 6 hours');
    
    // Run comprehensive test
    logWithTime('🔍 Running setup validation...');
    const testResult = runComprehensiveTest();
    
    if (testResult.success) {
      logWithTime('✅ Setup validation completed successfully');
      
      // Send setup confirmation with Job Titles support messaging
      try {
        GmailApp.sendEmail(
          Session.getActiveUser().getEmail(),
          '✅ LinkedIn Analytics Automation Setup Complete - WITH JOB TITLES',
          `LinkedIn Analytics Automation Setup Successful!
=========================================

🎯 SETUP SUMMARY:
✅ Automation trigger created (runs every 6 hours)
✅ Folder access validated
✅ Google Sheets access confirmed
✅ File processing tested
✨ Job Titles demographics support ENABLED

📁 CONFIGURATION:
- Incoming Folder: ${CONFIG.INCOMING_FOLDER_ID}
- Processed Folder: ${CONFIG.PROCESSED_FOLDER_ID}
- Total Sheets Configured: 24 (including Job Titles)

⚡ NEXT STEPS:
1. Upload your LinkedIn CSV files to the "Incoming CSV Files" folder
2. The system will automatically process them every 6 hours
3. Job Titles demographics will be automatically detected and processed
4. Check your email for processing summaries
5. View updated dashboards in Looker Studio

🔧 MANUAL CONTROLS:
- Run manually: Execute "runLinkedInAutomation()" function
- Quick test: Execute "quickTest()" function
- View logs: Check Google Apps Script execution transcript

🎉 Your LinkedIn analytics automation with Job Titles support is now live!
This is an automated setup confirmation from LinkedIn Analytics Automation.`
        );
      } catch (emailError) {
        logWithTime(`Setup email failed: ${emailError.message}`, 'WARN');
      }
      
    } else {
      throw new Error(`Setup validation failed: ${testResult.error}`);
    }
    
    logWithTime('🎉 LinkedIn Analytics automation with Job Titles support setup completed successfully!');
    return { success: true, message: 'Setup completed successfully with Job Titles support' };
    
  } catch (error) {
    logWithTime(`Setup failed: ${error.message}`, 'ERROR');
    
    try {
      GmailApp.sendEmail(
        Session.getActiveUser().getEmail(),
        '❌ LinkedIn Analytics Setup Failed',
        `LinkedIn Analytics Automation Setup Failed
=======================================

❌ Error: ${error.message}
🕐 Time: ${formatDate(new Date())}

🔧 Please check:
1. All folder IDs in CONFIG are correct and accessible
2. All Google Sheets IDs are valid and accessible (including Job Titles sheet)
3. Script has necessary permissions
4. Google Drive and Sheets APIs are enabled

Try running setupLinkedInAutomation() again after fixing the issues.`
      );
    } catch (emailError) {
      logWithTime(`Setup failure email failed: ${emailError.message}`, 'ERROR');
    }
    
    throw error;
  }
}

// ===================================================================
// SETUP AND TRIGGER MANAGEMENT
// ===================================================================

function setupLinkedInAutomation() {
  logWithTime('🚀 Setting up LinkedIn Analytics automation...');
  
  try {
    // Remove existing triggers to prevent duplicates
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === 'processAllLinkedInAnalytics') {
        ScriptApp.deleteTrigger(trigger);
        logWithTime('Removed existing trigger');
      }
    });
    
    // Create new trigger - runs every 6 hours
    ScriptApp.newTrigger('processAllLinkedInAnalytics')
             .timeBased()
             .everyHours(6)
             .create();
    
    logWithTime('✅ Automation trigger created - will run every 6 hours');
    
    // Run comprehensive test
    logWithTime('🔍 Running setup validation...');
    const testResult = runComprehensiveTest();
    
    if (testResult.success) {
      logWithTime('✅ Setup validation completed successfully');
      
      // Send setup confirmation
      try {
        GmailApp.sendEmail(
          Session.getActiveUser().getEmail(),
          '✅ LinkedIn Analytics Automation Setup Complete',
          `LinkedIn Analytics Automation Setup Successful!
=========================================

🎯 SETUP SUMMARY:
✅ Automation trigger created (runs every 6 hours)
✅ Folder access validated
✅ Google Sheets access confirmed
✅ File processing tested

📁 CONFIGURATION:
• Incoming Folder: ${CONFIG.INCOMING_FOLDER_ID}
• Processed Folder: ${CONFIG.PROCESSED_FOLDER_ID}
• Total Sheets Configured: 23

⚡ NEXT STEPS:
1. Upload your LinkedIn CSV files to the "Incoming CSV Files" folder
2. The system will automatically process them every 6 hours
3. Check your email for processing summaries
4. View updated dashboards in Looker Studio

🔧 MANUAL CONTROLS:
• Run manually: Execute "runLinkedInAutomation()" function
• Quick test: Execute "quickTest()" function
• View logs: Check Google Apps Script execution transcript

🎉 Your LinkedIn analytics automation is now live!
This is an automated setup confirmation from LinkedIn Analytics Automation.`
        );
      } catch (emailError) {
        logWithTime(`Setup email failed: ${emailError.message}`, 'WARN');
      }
      
    } else {
      throw new Error(`Setup validation failed: ${testResult.error}`);
    }
    
    logWithTime('🎉 LinkedIn Analytics automation setup completed successfully!');
    return { success: true, message: 'Setup completed successfully' };
    
  } catch (error) {
    logWithTime(`Setup failed: ${error.message}`, 'ERROR');
    
    try {
      GmailApp.sendEmail(
        Session.getActiveUser().getEmail(),
        '❌ LinkedIn Analytics Setup Failed',
        `LinkedIn Analytics Automation Setup Failed
=======================================

❌ Error: ${error.message}
🕐 Time: ${formatDate(new Date())}

🔧 Please check:
1. All folder IDs in CONFIG are correct and accessible
2. All Google Sheets IDs are valid and accessible  
3. Script has necessary permissions
4. Google Drive and Sheets APIs are enabled

Try running setupLinkedInAutomation() again after fixing the issues.`
      );
    } catch (emailError) {
      logWithTime(`Setup failure email failed: ${emailError.message}`, 'ERROR');
    }
    
    throw error;
  }
}

function runComprehensiveTest() {
  try {
    logWithTime('Running comprehensive setup test...');
    
    // Test 1: Configuration validation
    logWithTime('Test 1: Configuration validation');
    const requiredIds = [
      CONFIG.INCOMING_FOLDER_ID,
      CONFIG.PROCESSED_FOLDER_ID,
      CONFIG.CAMPAIGN_PERFORMANCE_SHEET_ID,
      CONFIG.MASTER_DASHBOARD_SHEET_ID
    ];
    
    for (const id of requiredIds) {
      if (!id || id.length < 20) {
        throw new Error(`Invalid configuration ID: ${id}`);
      }
    }
    logWithTime('✅ Configuration IDs valid');
    
    // Test 2: Folder access
    logWithTime('Test 2: Folder access validation');
    const folders = validateFolders();
    if (!folders) {
      throw new Error('Folder validation failed');
    }
    logWithTime('✅ Folder access validated');
    
    // Test 3: Sheet access
    logWithTime('Test 3: Google Sheets access validation');
    try {
      const testSheet = getOrCreateSheet(CONFIG.MASTER_DASHBOARD_SHEET_ID, CONFIG.DEFAULT_SHEET_NAME);
      testSheet.getRange('A1').setValue('Test');
      testSheet.getRange('A1').clearContent();
      logWithTime('✅ Sheet access validated');
    } catch (sheetError) {
      throw new Error(`Sheet access failed: ${sheetError.message}`);
    }
    
    // Test 4: File processing capabilities
    logWithTime('Test 4: File processing capabilities');
    const testFiles = folders.incomingFolder.getFiles();
    let fileCount = 0;
    while (testFiles.hasNext() && fileCount < 5) {
      testFiles.next();
      fileCount++;
    }
    logWithTime(`✅ Can access ${fileCount} files in incoming folder`);
    
    // Test 5: Memory and performance
    logWithTime('Test 5: Memory and performance check');
    const startTime = Date.now();
    const testData = [];
    for (let i = 0; i < 1000; i++) {
      testData.push({ test: i, data: `test_${i}` });
    }
    const endTime = Date.now();
    logWithTime(`✅ Performance test: ${endTime - startTime}ms for 1000 records`);
    
    logWithTime('✅ All tests passed successfully');
    return { success: true };
    
  } catch (error) {
    logWithTime(`❌ Test failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

// ===================================================================
// MANUAL EXECUTION AND TESTING FUNCTIONS
// ===================================================================

function runLinkedInAutomation() {
  logWithTime('🎯 Manual execution initiated');
  const result = processAllLinkedInAnalytics();
  
  if (result.success) {
    logWithTime(`✅ Manual execution completed successfully. Processed ${result.processed} files in ${result.time}s`);
  } else {
    logWithTime(`❌ Manual execution failed: ${result.error}`, 'ERROR');
  }
  
  return result;
}

function quickTest() {
  logWithTime('⚡ Running quick diagnostic test');
  const result = runComprehensiveTest();
  
  if (result.success) {
    logWithTime('✅ Quick test passed - system is ready');
  } else {
    logWithTime(`❌ Quick test failed: ${result.error}`, 'ERROR');
  }
  
  return result;
}

function debugSpecificFile(fileName) {
  try {
    logWithTime(`🔍 Debug analysis for file: ${fileName}`);
    
    const folders = validateFolders();
    if (!folders) {
      return { success: false, error: 'Folder validation failed' };
    }
    
    // Find the specific file
    const files = getFilesFromFolder(folders.incomingFolder);
    let targetFile = null;
    
    logWithTime(`Found ${files.length} files in folder`);
    
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      try {
        const currentName = file.getName();
        logWithTime(`Checking file ${i}: ${currentName}`);
        
        if (currentName === fileName) {
          targetFile = file;
          logWithTime(`✓ Found target file: ${fileName}`);
          break;
        }
      } catch (nameError) {
        logWithTime(`Error getting name for file ${i}: ${nameError.message}`, 'ERROR');
      }
    }
    
    if (!targetFile) {
      return { success: false, error: `File "${fileName}" not found in folder` };
    }
    
    // Debug the file thoroughly
    const debugInfo = debugFileAccess(targetFile, 'specific-debug');
    logWithTime(`File debug info: ${JSON.stringify(debugInfo, null, 2)}`);
    
    // Test file detection
    const isLinkedIn = isLinkedInFile(fileName);
    const fileType = detectFileType(fileName);
    const fileDate = extractDateFromFileName(fileName);
    
    logWithTime(`LinkedIn detection: ${isLinkedIn}`);
    logWithTime(`File type: ${fileType}`);
    logWithTime(`Date extraction: ${JSON.stringify(fileDate)}`);
    
    // Test file validation
    const validation = validateFile(targetFile);
    logWithTime(`File validation: ${JSON.stringify(validation)}`);
    
    // Test content reading
    try {
      const blob = targetFile.getBlob();
      const content = blob.getDataAsString();
      const contentPreview = content.substring(0, 200);
      logWithTime(`Content preview (first 200 chars): ${contentPreview}`);
      
      // Test CSV parsing
      const rows = content.split('\n').slice(0, 5);
      logWithTime(`First 5 rows: ${JSON.stringify(rows)}`);
      
    } catch (contentError) {
      logWithTime(`Content reading failed: ${contentError.message}`, 'ERROR');
    }
    
    return {
      success: true,
      debugInfo,
      isLinkedIn,
      fileType,
      fileDate,
      validation
    };
    
  } catch (error) {
    logWithTime(`Debug failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

function testSpecificFilename() {
  const fileName = "linkedin_follower_seniority_august2025";
  
  logWithTime(`Testing filename: ${fileName}`);
  const result = extractDateFromFileName(fileName);
  logWithTime(`Extraction result: ${JSON.stringify(result, null, 2)}`);
  
  const periodResult = extractPeriodDatesFromFilename(fileName);
  logWithTime(`Period result: Start: ${periodResult.period_start_date}, End: ${periodResult.period_end_date}`);
  
  return { extraction: result, period: periodResult };
}

// FIXED: Test function to validate the period date extraction
function testPeriodDateExtraction() {
  const testFiles = [
    'linkedin_campaign_performance_jul2025.csv',
    'linkedin_demographics_report_july2025.csv', 
    'linkedin_content_analytics_aug2025.csv',      // Test aug2025
    'linkedin_follower_metrics_august2025.csv',    // Test august2025
    'visitor_analytics_26aug2025.csv',             // Test day+month
    'campaign_data_15august2025.csv',              // Test day+full month
    'linkedin_visitor_metrics_2025_07.csv',
    'competitor_data_2025-07-15.csv',
    'campaign_data_week34_2025.csv'
  ];
  
  logWithTime('=== Testing period date extraction from filenames ===');
  
  testFiles.forEach(fileName => {
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    logWithTime(`\nFile: ${fileName}`);
    logWithTime(`Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    logWithTime(`Month: ${periodInfo.month_name} ${periodInfo.year}`);
    logWithTime(`Detected: ${periodInfo.detected} | Source: ${periodInfo.date_source}`);
    logWithTime(`Original: "${periodInfo.original}"`);
  });
  
  return true;
}

function testFollowerIndustryProcessing() {
  const fileName = "linkedin_follower_industry_july2025.csv"; // Your exact filename
  
  // Test the extraction first
  const dateInfo = extractDateFromFileName(fileName);
  logWithTime(`Date extraction: ${JSON.stringify(dateInfo)}`);
  
  const periodInfo = extractPeriodDatesFromFilename(fileName);
  logWithTime(`Period info: Start=${periodInfo.period_start_date}, End=${periodInfo.period_end_date}`);
  
  // Test what file type is detected
  const fileType = detectFileType(fileName);
  logWithTime(`Detected file type: ${fileType}`);
  
  // Check if the file exists in your incoming folder
  const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
  const files = incomingFolder.getFiles();
  
  while (files.hasNext()) {
    const file = files.next();
    if (file.getName() === fileName) {
      logWithTime(`Found file: ${fileName}`);
      
      // Test the actual processing function directly
      const result = processFollowerFileEnhancedAppend(file, fileType, dateInfo);
      logWithTime(`Processing result: ${result}`);
      
      return;
    }
  }
  
  logWithTime(`File ${fileName} not found in incoming folder`);
}
// Debug function to check specific follower file
function debugFollowerFileHeaders() {
  try {
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    let followerFile = null;
    while (files.hasNext()) {
      const file = files.next();
      const fileName = file.getName();
      if (fileName.includes('follower') && fileName.includes('industry')) {
        followerFile = file;
        break;
      }
    }
    
    if (!followerFile) {
      logWithTime('No follower industry file found for debugging');
      return;
    }
    
    const fileName = followerFile.getName();
    logWithTime(`Debugging headers for: ${fileName}`);
    
    // Test the processing step by step
    const content = followerFile.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    
    logWithTime(`CSV rows: ${rows ? rows.length : 'null'}`);
    if (rows && rows.length > 0) {
      logWithTime(`First row: ${JSON.stringify(rows[0])}`);
    }
    
    const data = convertRowsToObjects(rows, 0);
    logWithTime(`Converted objects: ${data.length}`);
    if (data.length > 0) {
      logWithTime(`First object keys: ${Object.keys(data[0]).join(', ')}`);
      logWithTime(`First object: ${JSON.stringify(data[0], null, 2)}`);
    }
    
    // Test the period extraction
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    logWithTime(`Period info: ${JSON.stringify(periodInfo, null, 2)}`);
    
    return { fileName, rows: rows ? rows.length : 0, data: data.length, periodInfo };
    
  } catch (error) {
    logWithTime(`Debug failed: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}

logWithTime('Headers fix functions loaded');

// Function to check what files are actually available
function debugAvailableFiles() {
  try {
    logWithTime('=== Checking available files in incoming folder ===');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    const fileList = [];
    let fileCount = 0;
    
    while (files.hasNext()) {
      const file = files.next();
      const fileName = file.getName();
      const fileType = detectFileType(fileName);
      const isLinkedIn = isLinkedInFile(fileName);
      
      fileList.push({
        name: fileName,
        type: fileType,
        isLinkedIn: isLinkedIn,
        size: file.getSize()
      });
      
      logWithTime(`File ${++fileCount}: ${fileName} | Type: ${fileType} | LinkedIn: ${isLinkedIn}`);
    }
    
    logWithTime(`Total files found: ${fileCount}`);
    
    // Look for any follower file
    const followerFiles = fileList.filter(f => f.name.toLowerCase().includes('follower'));
    logWithTime(`Follower files found: ${followerFiles.length}`);
    
    followerFiles.forEach(f => {
      logWithTime(`  - ${f.name} (${f.type})`);
    });
    
    return fileList;
    
  } catch (error) {
    logWithTime(`Error checking files: ${error.message}`, 'ERROR');
    return [];
  }
}

// Function to test any available follower file
function testAnyFollowerFile() {
  try {
    logWithTime('=== Testing any available follower file ===');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    let testFile = null;
    let testFileName = '';
    
    // Find any follower file
    while (files.hasNext()) {
      const file = files.next();
      const fileName = file.getName();
      
      if (fileName.toLowerCase().includes('follower')) {
        testFile = file;
        testFileName = fileName;
        break;
      }
    }
    
    if (!testFile) {
      logWithTime('No follower files found to test');
      return false;
    }
    
    logWithTime(`Testing file: ${testFileName}`);
    
    // Step 1: Test file reading
    const content = testFile.getBlob().getDataAsString();
    logWithTime(`Content length: ${content.length} characters`);
    logWithTime(`First 200 chars: ${content.substring(0, 200)}`);
    
    // Step 2: Test CSV parsing
    const rows = parseCSVSafely(content);
    logWithTime(`Parsed rows: ${rows ? rows.length : 'null'}`);
    
    if (rows && rows.length > 0) {
      logWithTime(`First row (potential headers): ${JSON.stringify(rows[0])}`);
      if (rows.length > 1) {
        logWithTime(`Second row (sample data): ${JSON.stringify(rows[1])}`);
      }
    }
    
    // Step 3: Test object conversion
    const data = convertRowsToObjects(rows, 0);
    logWithTime(`Converted to objects: ${data.length}`);
    
    if (data.length > 0) {
      const firstObject = data[0];
      const keys = Object.keys(firstObject);
      logWithTime(`Object keys (${keys.length}): ${keys.join(', ')}`);
      logWithTime(`First object sample: ${JSON.stringify(firstObject, null, 2)}`);
    }
    
    // Step 4: Test file type detection
    const fileType = detectFileType(testFileName);
    logWithTime(`Detected file type: ${fileType}`);
    
    // Step 5: Test period extraction
    const periodInfo = extractPeriodDatesFromFilename(testFileName);
    logWithTime(`Period info: Start=${periodInfo.period_start_date}, End=${periodInfo.period_end_date}`);
    
    return {
      fileName: testFileName,
      rowCount: rows ? rows.length : 0,
      objectCount: data.length,
      headers: data.length > 0 ? Object.keys(data[0]) : [],
      fileType: fileType,
      periodInfo: periodInfo
    };
    
  } catch (error) {
    logWithTime(`Test failed: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}
// ===================================================================
// PROCESS THE ACTUAL FOLLOWER FILE TO SEE WHAT'S HAPPENING
// ===================================================================

function processActualFollowerFile() {
  try {
    logWithTime('=== Processing actual follower file ===');
    
    const fileName = "linkedin_follower_company size_aug2025.csv";
    
    // Process the actual file
    const result = manuallyProcessSpecificFile(fileName);
    
    logWithTime(`Actual processing result: ${result}`);
    
    // Also check what's actually in the sheet after processing
    if (result) {
      checkSheetContents();
    }
    
    return result;
    
  } catch (error) {
    logWithTime(`Error processing actual file: ${error.message}`, 'ERROR');
    return false;
  }
}

function checkSheetContents() {
  try {
    logWithTime('=== Checking sheet contents after processing ===');
    
    const sheetId = CONFIG.FOLLOWER_COMPANY_SIZE_SHEET_ID;
    const sheet = getOrCreateSheet(sheetId, CONFIG.DEFAULT_SHEET_NAME);
    
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    
    logWithTime(`Sheet dimensions: ${lastRow} rows x ${lastCol} columns`);
    
    if (lastRow > 0 && lastCol > 0) {
      // Check headers (row 1)
      const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
      logWithTime(`Headers in sheet: ${headers.join(', ')}`);
      
      // Check first few data rows
      if (lastRow > 1) {
        const dataRows = Math.min(3, lastRow - 1);
        const sampleData = sheet.getRange(2, 1, dataRows, lastCol).getValues();
        logWithTime(`Sample data (${dataRows} rows):`);
        sampleData.forEach((row, index) => {
          logWithTime(`  Row ${index + 2}: ${row.join(' | ')}`);
        });
      }
    } else {
      logWithTime('Sheet appears to be empty');
    }
    
    return { lastRow, lastCol, hasData: lastRow > 0 };
    
  } catch (error) {
    logWithTime(`Error checking sheet contents: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}

// Function to manually test sheet writing
function testSheetWriting() {
  try {
    logWithTime('=== Testing sheet writing directly ===');
    
    const testHeaders = ['Company Size', 'Total Followers', 'Period Start Date', 'Month', 'Year'];
    const testData = [
      {
        'Company Size': '1-10',
        'Total Followers': '100',
        'Period Start Date': '2025-08-01',
        'Month': 'Aug',
        'Year': '2025'
      },
      {
        'Company Size': '11-50',
        'Total Followers': '200',
        'Period Start Date': '2025-08-01',
        'Month': 'Aug',
        'Year': '2025'
      }
    ];
    
    logWithTime(`Test headers: ${testHeaders.join(', ')}`);
    logWithTime(`Test data: ${JSON.stringify(testData, null, 2)}`);
    
    // Try to write to the sheet
    const success = updateSheetAppend(
      CONFIG.FOLLOWER_COMPANY_SIZE_SHEET_ID,
      CONFIG.DEFAULT_SHEET_NAME,
      testHeaders,
      testData,
      'Test: Follower Company Size'
    );
    
    logWithTime(`Test write result: ${success}`);
    
    if (success) {
      // Check what actually got written
      checkSheetContents();
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Test writing failed: ${error.message}`, 'ERROR');
    return false;
  }
}

// Function to see the raw CSV content
function inspectRawCSV() {
  try {
    logWithTime('=== Inspecting raw CSV content ===');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    let targetFile = null;
    while (files.hasNext()) {
      const file = files.next();
      if (file.getName() === "linkedin_follower_company size_aug2025.csv") {
        targetFile = file;
        break;
      }
    }
    
    if (!targetFile) {
      logWithTime('File not found');
      return false;
    }
    
    const content = targetFile.getBlob().getDataAsString();
    logWithTime(`Raw content (first 500 chars): ${content.substring(0, 500)}`);
    
    const lines = content.split('\n');
    logWithTime(`Total lines: ${lines.length}`);
    
    lines.slice(0, 5).forEach((line, index) => {
      logWithTime(`Line ${index + 1}: "${line}"`);
    });
    
    return { totalLines: lines.length, firstFiveLines: lines.slice(0, 5) };
    
  } catch (error) {
    logWithTime(`Error inspecting CSV: ${error.message}`, 'ERROR');
    return false;
  }
}

// Complete diagnostic
function completeHeadersDiagnostic() {
  logWithTime('=== COMPLETE HEADERS DIAGNOSTIC ===');
  
  // Step 1: Inspect the raw CSV
  const csvInspection = inspectRawCSV();
  
  // Step 2: Test sheet writing with simple data
  const writeTest = testSheetWriting();
  
  // Step 3: Process the actual file
  const processResult = processActualFollowerFile();
  
  logWithTime('=== DIAGNOSTIC COMPLETE ===');
  return {
    csvInspection,
    writeTest,
    processResult
  };
}

logWithTime('Diagnostic functions ready - run completeHeadersDiagnostic()');

// Function to manually process a specific file with detailed logging
function manuallyProcessSpecificFile(fileName) {
  try {
    logWithTime(`=== Manually processing file: ${fileName} ===`);
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    let targetFile = null;
    
    while (files.hasNext()) {
      const file = files.next();
      if (file.getName() === fileName) {
        targetFile = file;
        break;
      }
    }
    
    if (!targetFile) {
      logWithTime(`File ${fileName} not found`);
      return false;
    }
    
    const fileType = detectFileType(fileName);
    const fileDate = extractDateFromFileName(fileName);
    
    logWithTime(`Processing: ${fileName} as ${fileType}`);
    
    // Test the actual processing function
    const result = processFollowerFileEnhancedAppendFixed(targetFile, fileType, fileDate);
    
    logWithTime(`Processing result: ${result}`);
    
    return result;
    
  } catch (error) {
    logWithTime(`Manual processing failed: ${error.message}`, 'ERROR');
    return false;
  }
}

// Function to test headers specifically
function testHeadersCreation() {
  try {
    logWithTime('=== Testing headers creation ===');
    
    // Create sample data to test header processing
    const sampleData = [
      {
        'Industry': 'Technology',
        'Total followers': '1000',
        'Percentage': '25.5%'
      },
      {
        'Industry': 'Finance',
        'Total followers': '800',
        'Percentage': '20.2%'
      }
    ];
    
    logWithTime(`Sample data: ${JSON.stringify(sampleData, null, 2)}`);
    
    // Test enrichment process
    const periodInfo = {
      period_start_date: '2025-09-01',
      period_end_date: '2025-09-30',
      month_name: 'Sep',
      year: 2025,
      year_month: '2025-09',
      quarter: 3,
      days_in_period: 30,
      original: 'sep2025',
      date_source: 'test'
    };
    
    const enrichedData = sampleData.map(row => ({
      ...row,
      'Period Start Date': periodInfo.period_start_date,
      'Period End Date': periodInfo.period_end_date,
      'Month': periodInfo.month_name,
      'Year': periodInfo.year,
      'Year_Month': periodInfo.year_month,
      'Quarter': `Q${periodInfo.quarter}`,
      'Processing Date': formatDate(new Date()),
      'File Type': 'follower_industry'
    }));
    
    logWithTime(`Enriched data sample: ${JSON.stringify(enrichedData[0], null, 2)}`);
    
    // Test header extraction
    const headers = Object.keys(enrichedData[0] || {});
    logWithTime(`Extracted headers (${headers.length}): ${headers.join(', ')}`);
    
    // Test demographic processing
    const finalData = processFollowerDemographicsForLookerStudio(enrichedData, 'follower_industry', periodInfo, true);
    logWithTime(`Final processed data sample: ${JSON.stringify(finalData[0], null, 2)}`);
    
    const finalHeaders = Object.keys(finalData[0] || {});
    logWithTime(`Final headers (${finalHeaders.length}): ${finalHeaders.join(', ')}`);
    
    return {
      originalHeaders: Object.keys(sampleData[0]),
      enrichedHeaders: headers,
      finalHeaders: finalHeaders,
      sampleData: finalData[0]
    };
    
  } catch (error) {
    logWithTime(`Headers test failed: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}

// All-in-one debugging function
function debugHeadersIssue() {
  logWithTime('=== COMPREHENSIVE HEADERS DEBUGGING ===');
  
  // Step 1: Check available files
  const files = debugAvailableFiles();
  
  // Step 2: Test any follower file
  const testResult = testAnyFollowerFile();
  
  // Step 3: Test headers creation process
  const headersTest = testHeadersCreation();
  
  logWithTime('=== DEBUG SUMMARY ===');
  logWithTime(`Files found: ${files.length}`);
  logWithTime(`Test result: ${JSON.stringify(testResult)}`);
  logWithTime(`Headers test: ${JSON.stringify(headersTest)}`);
  
  return {
    files: files,
    testResult: testResult,
    headersTest: headersTest
  };
}

logWithTime('Debug functions loaded - run debugHeadersIssue() for comprehensive testing');

function debugFollowerIndustryFile() {
  // Replace with your exact filename
  const fileName = "linkedin_follower_industry_july2025.csv"; // Use your actual filename
  
  logWithTime(`Testing exact filename: ${fileName}`);
  
  // Test date extraction
  const dateResult = extractDateFromFileName(fileName);
  logWithTime(`Date extraction result: ${JSON.stringify(dateResult, null, 2)}`);
  
  // Test period extraction  
  const periodResult = extractPeriodDatesFromFilename(fileName);
  logWithTime(`Period result: Start: ${periodResult.period_start_date}, End: ${periodResult.period_end_date}`);
  logWithTime(`Date source: ${periodResult.date_source}`);
  
  return { dateResult, periodResult };
}

// Test just August files specifically
function testAugustFiles() {
  const augustFiles = [
    'linkedin_campaign_aug2025.csv',
    'linkedin_demographics_august2025.csv',
    'content_26aug2025.csv',
    'visitor_15august2025.csv'
  ];
  
  logWithTime('=== Testing August files specifically ===');
  
  augustFiles.forEach(fileName => {
    debugFilenamePatterns(fileName);
    logWithTime('\n' + '='.repeat(50));
  });
}
function testAugustPeriodDates() {
  const fileName = 'linkedin_campaign_aug2025.csv';
  const periodInfo = extractPeriodDatesFromFilename(fileName);
  
  logWithTime('Period Info for aug2025:');
  logWithTime(`Period Start: ${periodInfo.period_start_date}`);
  logWithTime(`Period End: ${periodInfo.period_end_date}`);
  logWithTime(`Month: ${periodInfo.month_name} ${periodInfo.year}`);
  logWithTime(`Year-Month: ${periodInfo.year_month}`);
  
  return periodInfo;
}

function debugFollowerFiles() {
  const testFiles = [
    "linkedin_follower_company size_jul2025.csv",
    "linkedin_follower_industry_jul25.csv", 
    "linkedin_follower_seniority_jul2025.csv",
    "linkedin_follower_job_function_jul2025.csv",
    "linkedin_follower_Location_jul2025.csv",
    "linkedin_follower_New followers_jul2025.csv"
  ];
  
  logWithTime("Testing follower file detection...");
  
  testFiles.forEach(fileName => {
    const isLinkedIn = isLinkedInFile(fileName);
    const fileType = detectFileType(fileName);
    
    logWithTime(`File: ${fileName}`);
    logWithTime(`  LinkedIn: ${isLinkedIn}, Type: ${fileType}`);
    
    // Test specific patterns
    Object.entries(FILE_PATTERNS).forEach(([type, pattern]) => {
      if (pattern.test(fileName)) {
        logWithTime(`  Matches pattern: ${type}`);
      }
    });
    logWithTime('---');
  });
  
  return true;
}
// Updated main working processor with period date enhancements
function processLinkedInFilesWorkingWithPeriods() {
  try {
    const startTime = Date.now();
    logWithTime('Starting LinkedIn file processor with Period Dates...');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
    
    const files = [];
    const fileIterator = incomingFolder.getFiles();
    
    // Get all files
    while (fileIterator.hasNext()) {
      const file = fileIterator.next();
      const fileName = file.getName();
      if (isLinkedInFile(fileName)) {
        files.push(file);
        logWithTime(`Found LinkedIn file: ${fileName}`);
      }
    }
    
    logWithTime(`Processing ${files.length} LinkedIn files with Period Dates`);
    
    const results = {
      processed: 0, errors: 0, campaign: 0, content: 0, visitor: 0,
      follower: 0, competitor: 0, demographics: 0,
      periods_processed: new Set()
    };
    
    // Process each file with period date enhancements
    files.forEach((file, index) => {
      try {
        const fileName = file.getName();
        const fileType = detectFileType(fileName);
        const periodInfo = extractPeriodDatesFromFilename(fileName);
        
        results.periods_processed.add(`${periodInfo.month_name} ${periodInfo.year}`);
        
        logWithTime(`[${index + 1}/${files.length}] Processing ${fileName} as ${fileType} (${periodInfo.period_start_date} to ${periodInfo.period_end_date})`);
        
        let success = false;
        
        switch (fileType) {
          case 'campaign_performance':
            success = processCampaignFileEnhanced(file, extractDateFromFileName(fileName));
            if (success) results.campaign++;
            break;
            
          case 'demographics_report':
            logWithTime(`*** PROCESSING DEMOGRAPHICS WITH JOB TITLES AND PERIOD DATES ***`);
            success = processDemographicsFileEnhanced(file, extractDateFromFileName(fileName));
            if (success) results.demographics++;
            break;
            
          case 'content_analytics':
            success = processContentFileEnhanced(file, extractDateFromFileName(fileName));
            if (success) results.content++;
            break;
            
          case 'visitor_metrics':
          case 'visitor_company_size':
          case 'visitor_industry':
          case 'visitor_seniority':
          case 'visitor_function':
          case 'visitor_location':
            success = processVisitorFileEnhanced(file, fileType, extractDateFromFileName(fileName));
            if (success) results.visitor++;
            break;
            
          case 'follower_metrics':
          case 'follower_company_size':
          case 'follower_industry':
          case 'follower_seniority':
          case 'follower_function':
          case 'follower_location':
            success = processFollowerFileEnhanced(file, fileType, extractDateFromFileName(fileName));
            if (success) results.follower++;
            break;
            
          case 'competitor_analytics':
            success = processCompetitorFileEnhanced(file, extractDateFromFileName(fileName));
            if (success) results.competitor++;
            break;
            
          default:
            logWithTime(`Unknown file type: ${fileType}`, 'WARN');
            return;
        }
        
        // Move file if successful
        if (success) {
          try {
            file.moveTo(processedFolder);
            results.processed++;
            logWithTime(`✓ Successfully processed and moved: ${fileName}`);
          } catch (moveError) {
            logWithTime(`File processed but move failed: ${moveError.message}`, 'WARN');
            results.processed++;
          }
        } else {
          results.errors++;
          logWithTime(`✗ Failed to process: ${fileName}`, 'ERROR');
        }
        
      } catch (fileError) {
        results.errors++;
        logWithTime(`Processing error for ${file.getName()}: ${fileError.message}`, 'ERROR');
      }
    });
    
    // Calculate final timing
    const processingTime = (Date.now() - startTime) / 1000;
    
    // Update master dashboard
    updateMasterDashboard(results);
    
    logWithTime(`Period date processing complete: ${results.processed} processed, ${results.errors} errors in ${processingTime.toFixed(1)}s`);
    logWithTime(`Periods processed: ${Array.from(results.periods_processed).join(', ')}`);
    logWithTime(`Details: Campaign=${results.campaign}, Content=${results.content}, Visitor=${results.visitor}, Follower=${results.follower}, Demographics=${results.demographics}, Competitor=${results.competitor}`);
    
    return {
      success: true,
      processed: results.processed,
      errors: results.errors,
      time: processingTime,
      periods: Array.from(results.periods_processed),
      details: results
    };
    
  } catch (error) {
    const processingTime = (Date.now() - startTime) / 1000;
    logWithTime(`Period date processor failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message, time: processingTime };
  }
}

// ===================================================================
// UNIFIED LINKEDIN PROCESSOR WITH HISTORICAL DATA AND FIXED HEADERS
// ===================================================================

// ===================================================================
// UNIFIED LINKEDIN PROCESSOR WITH HISTORICAL DATA AND FIXED HEADERS
// ===================================================================

// ===================================================================
// DIRECT REPLACEMENT OF MAIN PROCESSING FUNCTION
// ===================================================================

// Replace your existing processLinkedInFilesWithHistoricalBackfill function with this version
function processLinkedInFilesWithHistoricalBackfill() {
  try {
    const startTime = Date.now();
    logWithTime('Starting LinkedIn file processor with COMPLETE HEADERS for ALL follower types...');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
    
    const files = [];
    const fileIterator = incomingFolder.getFiles();
    
    // Get all files
    while (fileIterator.hasNext()) {
      const file = fileIterator.next();
      const fileName = file.getName();
      if (isLinkedInFile(fileName)) {
        files.push(file);
        logWithTime(`Found LinkedIn file: ${fileName}`);
      }
    }
    
    logWithTime(`Processing ${files.length} LinkedIn files with COMPLETE HEADERS`);
    
    const results = {
      processed: 0, errors: 0, campaign: 0, content: 0, visitor: 0,
      follower: 0, competitor: 0, demographics: 0,
      periods_processed: new Set()
    };
    
    // Process each file with complete header processing
    files.forEach((file, index) => {
      try {
        const fileName = file.getName();
        const fileType = detectFileType(fileName);
        const periodInfo = extractPeriodDatesFromFilename(fileName);
        results.periods_processed.add(`${periodInfo.month_name} ${periodInfo.year}`);
        
        logWithTime(`[${index + 1}/${files.length}] Processing ${fileName} as ${fileType} (${periodInfo.period_start_date} to ${periodInfo.period_end_date}) - COMPLETE HEADERS MODE`);
        
        let success = false;
        
        switch (fileType) {
          case 'campaign_performance':
            success = processCampaignFileEnhancedAppend(file, extractDateFromFileName(fileName));
            if (success) results.campaign++;
            break;
            
          case 'demographics_report':
            logWithTime(`*** PROCESSING DEMOGRAPHICS WITH JOB TITLES ***`);
            success = processDemographicsFileEnhancedAppend(file, extractDateFromFileName(fileName));
            if (success) results.demographics++;
            break;
            
          case 'content_analytics':
            success = processContentFileEnhancedAppend(file, extractDateFromFileName(fileName));
            if (success) results.content++;
            break;
            
          case 'visitor_metrics':
          case 'visitor_company_size':
          case 'visitor_industry':
          case 'visitor_seniority':
          case 'visitor_function':
          case 'visitor_location':
            success = processVisitorFileEnhancedAppend(file, fileType, extractDateFromFileName(fileName));
            if (success) results.visitor++;
            break;
            
          case 'follower_metrics':
          case 'follower_company_size':
          case 'follower_industry':
          case 'follower_seniority':
          case 'follower_function':
          case 'follower_location':
            // Use the complete headers processing function
            success = processFollowerFileCompleteHeaders(file, fileType, extractDateFromFileName(fileName));
            if (success) results.follower++;
            break;
            
          case 'competitor_analytics':
            success = processCompetitorFileEnhanced(file, extractDateFromFileName(fileName));
            if (success) results.competitor++;
            break;
            
          default:
            logWithTime(`Unknown file type: ${fileType}`, 'WARN');
            return;
        }
        
        // Move file if successful
        if (success) {
          try {
            file.moveTo(processedFolder);
            results.processed++;
            logWithTime(`✓ Successfully processed and moved: ${fileName} (COMPLETE HEADERS)`);
          } catch (moveError) {
            logWithTime(`File processed but move failed: ${moveError.message}`, 'WARN');
            results.processed++;
          }
        } else {
          results.errors++;
          logWithTime(`✗ Failed to process: ${fileName}`, 'ERROR');
        }
        
      } catch (fileError) {
        results.errors++;
        logWithTime(`Processing error for ${file.getName()}: ${fileError.message}`, 'ERROR');
      }
    });
    
    // Calculate final timing
    const processingTime = (Date.now() - startTime) / 1000;
    
    // Update dashboard
    updateMasterDashboard(results);
    
    logWithTime(`COMPLETE HEADERS processing complete: ${results.processed} processed, ${results.errors} errors in ${processingTime.toFixed(1)}s`);
    logWithTime(`Periods processed: ${Array.from(results.periods_processed).join(', ')}`);
    logWithTime(`Details: Campaign=${results.campaign}, Content=${results.content}, Visitor=${results.visitor}, Follower=${results.follower}, Demographics=${results.demographics}, Competitor=${results.competitor}`);
    logWithTime(`*** ALL FOLLOWER FILES PROCESSED WITH COMPLETE 32+ HEADERS ***`);
    
    return {
      success: true,
      processed: results.processed,
      errors: results.errors,
      time: processingTime,
      periods: Array.from(results.periods_processed),
      details: results
    };
    
  } catch (error) {
    const processingTime = (Date.now() - startTime) / 1000;
    logWithTime(`COMPLETE HEADERS processor failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message, time: processingTime };
  }
}

// NEW FUNCTION: Complete headers processing for ALL follower types
function processFollowerFileCompleteHeaders(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing follower file (COMPLETE HEADERS): ${fileName} (${fileType})`);
    
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime(`No follower data found in ${fileType}`, 'WARN');
      return false;
    }
    
    const originalColumns = Object.keys(data[0]);
    logWithTime(`Original columns for ${fileType}: ${originalColumns.join(', ')}`);
    
    const isLatestPeriod = isCurrentOrMostRecentMonth(periodInfo);
    
    // FORCE complete processing for ALL follower demographic types
    let processedData;
    
    if (['follower_industry', 'follower_company_size', 'follower_seniority', 'follower_function', 'follower_location'].includes(fileType)) {
      
      logWithTime(`FORCING complete demographic processing for ${fileType}`);
      
      // Auto-detect columns
      let segmentColumn = originalColumns[0]; // First column is usually the segment
      let followersColumn = originalColumns[1]; // Second column is usually followers
      
      // Try to find better matches
      for (const col of originalColumns) {
        if (col.toLowerCase().includes('follower')) {
          followersColumn = col;
        }
      }
      
      logWithTime(`Using columns - Segment: "${segmentColumn}", Followers: "${followersColumn}"`);
      
      // Calculate total followers
      const totalFollowers = data.reduce((sum, row) => {
        return sum + parseInt(row[followersColumn] || 0);
      }, 0);
      
      logWithTime(`Total followers for ${fileType}: ${totalFollowers}`);
      
      // Create COMPLETE data with ALL required headers
      processedData = data.map(row => {
        const segmentFollowers = parseInt(row[followersColumn] || 0);
        const segmentPercentage = totalFollowers > 0 ? Math.round((segmentFollowers / totalFollowers) * 10000) / 100 : 0;
        
        return {
          // Original data
          ...row,
          
          // Period information
          'Period Start Date': periodInfo.period_start_date,
          'Period End Date': periodInfo.period_end_date,
          'Month': periodInfo.month_name,
          'Year': periodInfo.year,
          'Year_Month': periodInfo.year_month,
          'Quarter': `Q${periodInfo.quarter}`,
          'Days in Period': periodInfo.days_in_period,
          'Report Date': periodInfo.original,
          'Processing Date': formatDate(new Date()),
          'Date Source': periodInfo.date_source,
          'File Type': fileType,
          'Week': periodInfo.week,
          
          // Looker Studio fields - FORCE ALL OF THESE
          'Date_Key': periodInfo.period_start_date,
          'Month_Name': periodInfo.month_name,
          'Is_Latest_Available': isLatestPeriod,
          'Display_Priority': isLatestPeriod ? 1 : 0,
          'Period_Type': 'monthly',
          'Data_Freshness': isLatestPeriod ? 'current' : 'historical',
          
          // Demographic values - FORCE ALL OF THESE
          'Segment_Followers': segmentFollowers,
          'Segment_Percentage': segmentPercentage,
          'Total_Follower_Base': totalFollowers,
          'Previous_Month_Total': 0,
          'Segment_Name': row[segmentColumn] || '',
          'Segment_Value': row[segmentColumn] || '',
          
          // Metadata - FORCE ALL OF THESE
          'Report_Type': fileType,
          'Data_Source_File': `${fileType}_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
          'Upload_Date': formatDate(new Date()),
          'Upload_Time': new Date().toTimeString().split(' ')[0],
          
          // Current and historical fields - FORCE ALL OF THESE
          'Current_Total': segmentFollowers,
          'Current_Percentage': segmentPercentage,
          'Historical_Total': segmentFollowers,
          'Historical_Percentage': segmentPercentage,
          
          // Context fields - FORCE ALL OF THESE
          'Data_Type': isLatestPeriod ? 'Current Composition' : 'Historical Reference',
          'Display_Context': isLatestPeriod ? 'single_month' : 'range_historical'
        };
      });
      
    } else {
      // For follower_metrics
      processedData = data.map(row => ({
        ...row,
        'Period Start Date': periodInfo.period_start_date,
        'Period End Date': periodInfo.period_end_date,
        'Month': periodInfo.month_name,
        'Year': periodInfo.year,
        'Year_Month': periodInfo.year_month,
        'Quarter': `Q${periodInfo.quarter}`,
        'Days in Period': periodInfo.days_in_period,
        'Report Date': periodInfo.original,
        'Processing Date': formatDate(new Date()),
        'Date Source': periodInfo.date_source,
        'File Type': fileType,
        'Week': periodInfo.week
      }));
    }
    
    const sheetId = getFollowerSheetId(fileType);
    if (!sheetId) {
      logWithTime(`No sheet ID found for ${fileType}`, 'ERROR');
      return false;
    }
    
    const headers = Object.keys(processedData[0]);
    logWithTime(`COMPLETE HEADERS for ${fileType} (${headers.length}): ${headers.join(', ')}`);
    
    // Use append mode for historical data
    const success = updateSheetAppend(
      sheetId,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      processedData,
      `Follower: ${fileType} (Complete Headers)`
    );
    
    if (success) {
      logWithTime(`✓ ${fileType} processed with ${headers.length} complete headers: ${processedData.length} rows`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Complete headers processing error (${fileType}): ${error.message}`, 'ERROR');
    return false;
  }
}

logWithTime('Complete headers processing function loaded');

// UNIFIED FOLLOWER PROCESSING - Uses flexible column detection
function processFollowerFileUnified(file, fileType, fileDate) {
  try {
    const fileName = file.getName();
    const periodInfo = extractPeriodDatesFromFilename(fileName);
    
    logWithTime(`Processing follower file (UNIFIED): ${fileName} (${fileType}) - Period: ${periodInfo.period_start_date} to ${periodInfo.period_end_date}`);
    
    const content = file.getBlob().getDataAsString();
    const rows = parseCSVSafely(content);
    if (!rows) return false;
    
    const data = convertRowsToObjects(rows, 0);
    if (data.length === 0) {
      logWithTime(`No follower data found in ${fileType}`, 'WARN');
      return false;
    }
    
    // Debug the original data structure
    logWithTime(`Original columns for ${fileType}: ${Object.keys(data[0]).join(', ')}`);
    logWithTime(`Sample data: ${JSON.stringify(data[0], null, 2)}`);
    
    let processedData;
    const isLatestPeriod = isCurrentOrMostRecentMonth(periodInfo);
    
    if (['follower_industry', 'follower_company_size', 'follower_seniority', 'follower_function', 'follower_location'].includes(fileType)) {
      // Handle demographic processing with flexible column mapping
      processedData = processFollowerDemographicsFlexible(data, fileType, periodInfo, isLatestPeriod);
    } else {
      // For follower_metrics, add standard enhancements
      processedData = data.map(row => ({
        ...row,
        'Period Start Date': periodInfo.period_start_date,
        'Period End Date': periodInfo.period_end_date,
        'Month': periodInfo.month_name,
        'Year': periodInfo.year,
        'Year_Month': periodInfo.year_month,
        'Quarter': `Q${periodInfo.quarter}`,
        'Days in Period': periodInfo.days_in_period,
        'Report Date': periodInfo.original,
        'Processing Date': formatDate(new Date()),
        'Date Source': periodInfo.date_source,
        'File Type': fileType,
        'Week': periodInfo.week,
        'Date_Key': periodInfo.period_start_date,
        'Month_Name': periodInfo.month_name,
        'Is_Latest_Available': isLatestPeriod,
        'Display_Priority': isLatestPeriod ? 1 : 0,
        'Period_Type': 'monthly',
        'Data_Freshness': isLatestPeriod ? 'current' : 'historical',
        'Report_Type': fileType,
        'Data_Source_File': `${fileType}_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
        'Upload_Date': formatDate(new Date()),
        'Upload_Time': new Date().toTimeString().split(' ')[0]
      }));
    }
    
    const sheetId = getFollowerSheetId(fileType);
    if (!sheetId) {
      logWithTime(`No sheet ID found for ${fileType}`, 'ERROR');
      return false;
    }
    
    const headers = Object.keys(processedData[0]);
    logWithTime(`Final headers for ${fileType} (${headers.length}): ${headers.join(', ')}`);
    
    // Use append mode for historical data
    const success = updateSheetAppend(
      sheetId,
      CONFIG.DEFAULT_SHEET_NAME,
      headers,
      processedData,
      `Follower: ${fileType} (Unified)`
    );
    
    if (success) {
      logWithTime(`✓ ${fileType} processed with unified headers: ${processedData.length} rows`);
    }
    
    return success;
    
  } catch (error) {
    logWithTime(`Unified follower processing error (${fileType}): ${error.message}`, 'ERROR');
    return false;
  }
}

// NEW FUNCTION: Flexible demographic processing that handles different column structures
function processFollowerDemographicsFlexible(data, fileType, periodInfo, isLatestPeriod) {
  try {
    // Detect the actual column names in this file
    const originalColumns = Object.keys(data[0]);
    logWithTime(`Processing ${fileType} with columns: ${originalColumns.join(', ')}`);
    
    // Find the segment/category column (first non-numeric column)
    let segmentColumn = null;
    let followersColumn = null;
    let percentageColumn = null;
    
    // Common patterns for different file types
    const columnPatterns = {
      follower_industry: {
        segment: ['Industry', 'industry', 'Industry Name', 'industry_name'],
        followers: ['Total followers', 'total followers', 'Total Followers', 'Followers'],
        percentage: ['Percentage', 'percentage', '%', 'Percent']
      },
      follower_company_size: {
        segment: ['Company size', 'company size', 'Company Size', 'Size'],
        followers: ['Total followers', 'total followers', 'Total Followers', 'Followers'],
        percentage: ['Percentage', 'percentage', '%', 'Percent']
      },
      follower_seniority: {
        segment: ['Seniority', 'seniority', 'Job Seniority', 'Level'],
        followers: ['Total followers', 'total followers', 'Total Followers', 'Followers'],
        percentage: ['Percentage', 'percentage', '%', 'Percent']
      },
      follower_function: {
        segment: ['Function', 'function', 'Job Function', 'Role'],
        followers: ['Total followers', 'total followers', 'Total Followers', 'Followers'],
        percentage: ['Percentage', 'percentage', '%', 'Percent']
      },
      follower_location: {
        segment: ['Location', 'location', 'Country', 'Region'],
        followers: ['Total followers', 'total followers', 'Total Followers', 'Followers'],
        percentage: ['Percentage', 'percentage', '%', 'Percent']
      }
    };
    
    const patterns = columnPatterns[fileType] || columnPatterns.follower_company_size;
    
    // Find matching columns
    for (const col of originalColumns) {
      if (!segmentColumn && patterns.segment.some(pattern => col.toLowerCase().includes(pattern.toLowerCase()))) {
        segmentColumn = col;
      }
      if (!followersColumn && patterns.followers.some(pattern => col.toLowerCase().includes(pattern.toLowerCase()))) {
        followersColumn = col;
      }
      if (!percentageColumn && patterns.percentage.some(pattern => col.toLowerCase().includes(pattern.toLowerCase()))) {
        percentageColumn = col;
      }
    }
    
    // Fallback: use first three columns if pattern matching fails
    if (!segmentColumn) segmentColumn = originalColumns[0];
    if (!followersColumn) followersColumn = originalColumns[1];
    if (!percentageColumn && originalColumns.length > 2) percentageColumn = originalColumns[2];
    
    logWithTime(`Detected columns - Segment: "${segmentColumn}", Followers: "${followersColumn}", Percentage: "${percentageColumn || 'none'}"`);
    
    // Calculate total followers for percentage calculations
    const totalFollowers = data.reduce((sum, row) => {
      const followers = parseInt(row[followersColumn] || 0);
      return sum + followers;
    }, 0);
    
    logWithTime(`Total followers for ${fileType}: ${totalFollowers}`);
    
    // Process each row with all required fields
    const processedData = data.map(row => {
      const segmentFollowers = parseInt(row[followersColumn] || 0);
      const segmentPercentage = totalFollowers > 0 ? Math.round((segmentFollowers / totalFollowers) * 10000) / 100 : 0;
      
      // Create standardized output with all original data preserved
      const standardizedRow = {
        // Original data - preserve all original columns
        ...row,
        
        // Add standardized segment name for consistency
        'Segment_Name': row[segmentColumn] || '',
        'Segment_Value': row[segmentColumn] || '',
        
        // Period information
        'Period Start Date': periodInfo.period_start_date,
        'Period End Date': periodInfo.period_end_date,
        'Month': periodInfo.month_name,
        'Year': periodInfo.year,
        'Year_Month': periodInfo.year_month,
        'Quarter': `Q${periodInfo.quarter}`,
        'Days in Period': periodInfo.days_in_period,
        'Report Date': periodInfo.original,
        'Processing Date': formatDate(new Date()),
        'Date Source': periodInfo.date_source,
        'File Type': fileType,
        'Week': periodInfo.week,
        
        // Looker Studio fields
        'Date_Key': periodInfo.period_start_date,
        'Month_Name': periodInfo.month_name,
        'Is_Latest_Available': isLatestPeriod,
        'Display_Priority': isLatestPeriod ? 1 : 0,
        'Period_Type': 'monthly',
        'Data_Freshness': isLatestPeriod ? 'current' : 'historical',
        
        // Demographic values
        'Segment_Followers': segmentFollowers,
        'Segment_Percentage': segmentPercentage,
        'Total_Follower_Base': totalFollowers,
        'Previous_Month_Total': 0,
        
        // Metadata
        'Report_Type': fileType,
        'Data_Source_File': `${fileType}_${periodInfo.month_name.toLowerCase()}${periodInfo.year}.csv`,
        'Upload_Date': formatDate(new Date()),
        'Upload_Time': new Date().toTimeString().split(' ')[0],
        
        // Current and historical fields
        'Current_Total': segmentFollowers,
        'Current_Percentage': segmentPercentage,
        'Historical_Total': segmentFollowers,
        'Historical_Percentage': segmentPercentage,
        
        // Context fields
        'Data_Type': isLatestPeriod ? 'Current Composition' : 'Historical Reference',
        'Display_Context': isLatestPeriod ? 'single_month' : 'range_historical'
      };
      
      return standardizedRow;
    });
    
    logWithTime(`Processed ${processedData.length} rows for ${fileType}`);
    logWithTime(`Sample processed row keys: ${Object.keys(processedData[0]).join(', ')}`);
    
    return processedData;
    
  } catch (error) {
    logWithTime(`Flexible demographic processing error: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Add other unified processing functions for consistency
function processCampaignFileUnified(file, fileDate) {
  // Use the existing enhanced append function
  return processCampaignFileEnhancedAppend(file, fileDate);
}

function processDemographicsFileUnified(file, fileDate) {
  // Use the existing enhanced append function
  return processDemographicsFileEnhancedAppend(file, fileDate);
}

function processContentFileUnified(file, fileDate) {
  // Use the existing enhanced append function
  return processContentFileEnhancedAppend(file, fileDate);
}

function processVisitorFileUnified(file, fileType, fileDate) {
  // Use the existing enhanced append function
  return processVisitorFileEnhancedAppend(file, fileType, fileDate);
}

function processCompetitorFileUnified(file, fileDate) {
  // Use the existing competitor function (add enhanced append if needed)
  return processCompetitorFileEnhanced(file, fileDate);
}

// Export the main function
var processLinkedInFilesComplete = processLinkedInFilesWithHistoricalBackfill;

logWithTime('Unified LinkedIn processor with historical data and fixed headers loaded');
// Add other unified processing functions for consistency
function processCampaignFileUnified(file, fileDate) {
  // Use the existing enhanced append function
  return processCampaignFileEnhancedAppend(file, fileDate);
}

function processDemographicsFileUnified(file, fileDate) {
  // Use the existing enhanced append function
  return processDemographicsFileEnhancedAppend(file, fileDate);
}

function processContentFileUnified(file, fileDate) {
  // Use the existing enhanced append function
  return processContentFileEnhancedAppend(file, fileDate);
}

function processVisitorFileUnified(file, fileType, fileDate) {
  // Use the existing enhanced append function
  return processVisitorFileEnhancedAppend(file, fileType, fileDate);
}

function processCompetitorFileUnified(file, fileDate) {
  // Use the existing competitor function (add enhanced append if needed)
  return processCompetitorFileEnhanced(file, fileDate);
}

// Export the main function
var processLinkedInFilesComplete = processLinkedInFilesWithHistoricalBackfill;

logWithTime('Unified LinkedIn processor with historical data and fixed headers loaded');
function processLinkedInFilesWorking() {
  try {
    const startTime = Date.now(); // Use local timing instead of global state
    logWithTime('Starting working LinkedIn file processor with Job Titles support...');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
    
    const files = [];
    const fileIterator = incomingFolder.getFiles();
    
    // Get all files
    while (fileIterator.hasNext()) {
      const file = fileIterator.next();
      const fileName = file.getName();
      if (isLinkedInFile(fileName)) {
        files.push(file);
        logWithTime(`Found LinkedIn file: ${fileName}`);
      }
    }
    
    logWithTime(`Processing ${files.length} LinkedIn files`);
    
    const results = {
      processed: 0,
      errors: 0,
      campaign: 0,
      content: 0,
      visitor: 0,
      follower: 0,
      competitor: 0,
      demographics: 0
    };
    
    // Process each file
    files.forEach((file, index) => {
      try {
        const fileName = file.getName();
        const fileType = detectFileType(fileName);
        const fileDate = extractDateFromFileName(fileName);
        
        logWithTime(`[${index + 1}/${files.length}] Processing ${fileName} as ${fileType}`);
        
        let success = false;
        
        switch (fileType) {
          case 'campaign_performance':
            success = processCampaignFile(file, fileDate);
            if (success) results.campaign++;
            break;
            
          case 'demographics_report':
            logWithTime(`*** PROCESSING DEMOGRAPHICS WITH JOB TITLES SUPPORT ***`);
            success = processDemographicsFile(file, fileDate);
            if (success) results.demographics++;
            break;
            
          case 'content_analytics':
            success = processContentFile(file, fileDate);
            if (success) results.content++;
            break;
            
          case 'visitor_metrics':
          case 'visitor_company_size':
          case 'visitor_industry':
          case 'visitor_seniority':
          case 'visitor_function':
          case 'visitor_location':
            success = processVisitorFile(file, fileType, fileDate);
            if (success) results.visitor++;
            break;
            
          case 'follower_metrics':
          case 'follower_company_size':
          case 'follower_industry':
          case 'follower_seniority':
          case 'follower_function':
          case 'follower_location':
            success = processFollowerFile(file, fileType, fileDate);
            if (success) results.follower++;
            break;
            
          case 'competitor_analytics':
            success = processCompetitorFile(file, fileDate);
            if (success) results.competitor++;
            break;
            
          default:
            logWithTime(`Unknown file type: ${fileType}`, 'WARN');
            return;
        }
        
        // Move file if successful
        if (success) {
          try {
            file.moveTo(processedFolder);
            results.processed++;
            logWithTime(`✓ Successfully processed and moved: ${fileName}`);
          } catch (moveError) {
            logWithTime(`File processed but move failed: ${moveError.message}`, 'WARN');
            results.processed++;
          }
        } else {
          results.errors++;
          logWithTime(`✗ Failed to process: ${fileName}`, 'ERROR');
        }
        
      } catch (fileError) {
        results.errors++;
        logWithTime(`Processing error for ${file.getName()}: ${fileError.message}`, 'ERROR');
      }
    });
    
    // Calculate final timing
    const processingTime = (Date.now() - startTime) / 1000;
    
    // Update dashboard
    updateMasterDashboard(results);
    
    logWithTime(`Processing complete: ${results.processed} processed, ${results.errors} errors in ${processingTime.toFixed(1)}s`);
    logWithTime(`Details: Campaign=${results.campaign}, Content=${results.content}, Visitor=${results.visitor}, Follower=${results.follower}, Demographics=${results.demographics}, Competitor=${results.competitor}`);
    
    return {
      success: true,
      processed: results.processed,
      errors: results.errors,
      time: processingTime,
      details: results
    };
    
  } catch (error) {
    const processingTime = (Date.now() - startTime) / 1000;
    logWithTime(`Working processor failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message, time: processingTime };
  }
}
// Updated main temporal processor to use all temporal functions
function processLinkedInFilesWorkingTemporal() {
  try {
    const startTime = Date.now();
    logWithTime('Starting temporal LinkedIn file processor with Job Titles support...');
    
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
    
    const files = [];
    const fileIterator = incomingFolder.getFiles();
    
    // Get all files
    while (fileIterator.hasNext()) {
      const file = fileIterator.next();
      const fileName = file.getName();
      if (isLinkedInFile(fileName)) {
        files.push(file);
        logWithTime(`Found LinkedIn file: ${fileName}`);
      }
    }
    
    logWithTime(`Processing ${files.length} LinkedIn files with temporal tracking`);
    
    const results = {
      processed: 0, errors: 0, campaign: 0, content: 0, visitor: 0,
      follower: 0, competitor: 0, demographics: 0,
      months_processed: new Set()
    };
    
    // Process each file with temporal tracking
    files.forEach((file, index) => {
      try {
        const fileName = file.getName();
        const fileType = detectFileType(fileName);
        const temporalInfo = extractTemporalInfo(fileName);
        results.months_processed.add(temporalInfo.year_month);
        
        logWithTime(`[${index + 1}/${files.length}] Processing ${fileName} as ${fileType} (${temporalInfo.month_name} ${temporalInfo.year})`);
        
        let success = false;
        
        switch (fileType) {
          case 'campaign_performance':
            success = processCampaignFileTemporal(file, extractDateFromFileName(fileName));
            if (success) results.campaign++;
            break;
            
          case 'demographics_report':
            logWithTime(`*** PROCESSING DEMOGRAPHICS WITH JOB TITLES AND TEMPORAL SUPPORT ***`);
            success = processDemographicsFileTemporal(file, extractDateFromFileName(fileName));
            if (success) results.demographics++;
            break;
            
          case 'content_analytics':
            success = processContentFileTemporal(file, extractDateFromFileName(fileName));
            if (success) results.content++;
            break;
            
          case 'visitor_metrics':
          case 'visitor_company_size':
          case 'visitor_industry':
          case 'visitor_seniority':
          case 'visitor_function':
          case 'visitor_location':
            success = processVisitorFileTemporal(file, fileType, extractDateFromFileName(fileName));
            if (success) results.visitor++;
            break;
            
          case 'follower_metrics':
          case 'follower_company_size':
          case 'follower_industry':
          case 'follower_seniority':
          case 'follower_function':
          case 'follower_location':
            success = processFollowerFileTemporal(file, fileType, extractDateFromFileName(fileName));
            if (success) results.follower++;
            break;
            
          case 'competitor_analytics':
            success = processCompetitorFileTemporal(file, extractDateFromFileName(fileName));
            if (success) results.competitor++;
            break;
            
          default:
            logWithTime(`Unknown file type: ${fileType}`, 'WARN');
            return;
        }
        
        // Move file if successful
        if (success) {
          try {
            file.moveTo(processedFolder);
            results.processed++;
            logWithTime(`✓ Successfully processed and moved: ${fileName}`);
          } catch (moveError) {
            logWithTime(`File processed but move failed: ${moveError.message}`, 'WARN');
            results.processed++;
          }
        } else {
          results.errors++;
          logWithTime(`✗ Failed to process: ${fileName}`, 'ERROR');
        }
        
      } catch (fileError) {
        results.errors++;
        logWithTime(`Processing error for ${file.getName()}: ${fileError.message}`, 'ERROR');
      }
    });
    
    // Calculate final timing
    const processingTime = (Date.now() - startTime) / 1000;
    
    // Update dashboards (both regular and temporal)
    updateMasterDashboard(results);
    updateMonthlyComparisonDashboard();
    
    logWithTime(`Temporal processing complete: ${results.processed} processed, ${results.errors} errors in ${processingTime.toFixed(1)}s`);
    logWithTime(`Months processed: ${Array.from(results.months_processed).join(', ')}`);
    logWithTime(`Details: Campaign=${results.campaign}, Content=${results.content}, Visitor=${results.visitor}, Follower=${results.follower}, Demographics=${results.demographics}, Competitor=${results.competitor}`);
    
    return {
      success: true,
      processed: results.processed,
      errors: results.errors,
      time: processingTime,
      months: Array.from(results.months_processed),
      details: results
    };
    
  } catch (error) {
    const processingTime = (Date.now() - startTime) / 1000;
    logWithTime(`Temporal working processor failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message, time: processingTime };
  }
}
// Debug function to test specific filename patterns
// Debug function to test specific filename patterns
function debugFilenamePatterns(fileName) {
  logWithTime(`\n=== DEBUGGING: ${fileName} ===`);
  
  const monthMap = {
    'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
    'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6,
    'jul': 7, 'july': 7, 'aug': 8, 'august': 8, 'sep': 9, 'september': 9,
    'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12
  };
  
  const patterns = [
    { name: 'Full month + year', regex: /\b(january|february|march|april|may|june|july|august|september|october|november|december)(\d{4})\b/i },
    { name: 'Short month + year', regex: /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})\b/i },
    { name: 'Day + full month', regex: /(\d{1,2})(january|february|march|april|may|june|july|august|september|october|november|december)(\d{4})?/i },
    { name: 'Day + short month', regex: /(\d{1,2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})?/i }
  ];
  
  patterns.forEach(pattern => {
    const match = fileName.match(pattern.regex);
    if (match) {
      logWithTime(`✓ ${pattern.name} MATCHED: ${JSON.stringify(match)}`);
      
      if (match[1] && monthMap[match[1].toLowerCase()]) {
        const month = monthMap[match[1].toLowerCase()];
        logWithTime(`  Month "${match[1]}" -> ${month} (${getMonthName(month)})`);
      }
      if (match[2]) {
        logWithTime(`  Year: ${match[2]}`);
      }
    } else {
      logWithTime(`✗ ${pattern.name} no match`);
    }
  });
  
  // Test the actual extraction
  logWithTime('\n--- Running actual extraction ---');
  const result = extractDateFromFileName(fileName);
  logWithTime(`Result: ${JSON.stringify(result, null, 2)}`);
  
  return result;
}

function testTemporalProcessing() {
  try {
    logWithTime('Testing temporal processing on available files...');
    
    // This will process files with temporal tracking
    const result = processLinkedInFilesWorkingTemporal();
    
    logWithTime(`Temporal test result: ${JSON.stringify(result)}`);
    
    if (result.success) {
      logWithTime(`✅ Temporal test completed: ${result.processed} files processed across ${result.months ? result.months.length : 0} months`);
      if (result.months) {
        logWithTime(`Months processed: ${result.months.join(', ')}`);
      }
    } else {
      logWithTime(`❌ Temporal test failed: ${result.error}`, 'ERROR');
    }
    
    return result;
    
  } catch (error) {
    logWithTime(`Temporal test exception: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}
// ===================================================================
// CONFIGURATION MANAGEMENT
// ===================================================================

function updateConfiguration(newConfig) {
  try {
    logWithTime('🔧 Updating configuration...');
    
    const properties = PropertiesService.getScriptProperties();
    let updateCount = 0;
    
    Object.keys(newConfig).forEach(key => {
      if (CONFIG.hasOwnProperty(key)) {
        properties.setProperty(`CONFIG_${key}`, newConfig[key].toString());
        updateCount++;
      } else {
        logWithTime(`Unknown config key: ${key}`, 'WARN');
      }
    });
    
    logWithTime(`✅ Configuration updated: ${updateCount} properties changed`);
    return { success: true, updated: updateCount };
    
  } catch (error) {
    logWithTime(`❌ Configuration update failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

function getSystemStatus() {
  try {
    const status = {
      timestamp: new Date().toISOString(),
      triggers: ScriptApp.getProjectTriggers().length,
      folders: null,
      lastExecution: null,
      configuration: {
        incoming_folder: CONFIG.INCOMING_FOLDER_ID,
        processed_folder: CONFIG.PROCESSED_FOLDER_ID,
        total_sheets: 23
      }
    };
    
    // Check folder access
    try {
      const folders = validateFolders();
      status.folders = folders ? 'accessible' : 'error';
    } catch (e) {
      status.folders = 'error';
    }
    
    // Check last execution from properties
    try {
      const properties = PropertiesService.getScriptProperties();
      status.lastExecution = properties.getProperty('LAST_EXECUTION') || 'never';
    } catch (e) {
      status.lastExecution = 'unknown';
    }
    
    return status;
    
  } catch (error) {
    logWithTime(`System status check failed: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}

function recordExecution(result) {
  try {
    const properties = PropertiesService.getScriptProperties();
    const executionData = {
      timestamp: new Date().toISOString(),
      success: result.success,
      processed: result.processed || 0,
      errors: result.errors || 0,
      time: result.time || 0
    };
    
    properties.setProperties({
      'LAST_EXECUTION': executionData.timestamp,
      'LAST_RESULT': JSON.stringify(executionData)
    });
    
  } catch (error) {
    logWithTime(`Failed to record execution: ${error.message}`, 'WARN');
  }
}

// ===================================================================
// ENHANCED ERROR HANDLING
// ===================================================================

function handleCriticalError(error, context = 'Unknown') {
  const errorReport = {
    context,
    message: error.message,
    stack: error.stack,
    timestamp: new Date().toISOString(),
    memoryUsage: processingState.memoryUsage,
    processedCount: processingState.processedCount,
    processingTime: processingState.startTime ? Date.now() - processingState.startTime : 0
  };
  
  logWithTime(`💥 CRITICAL ERROR in ${context}: ${error.message}`, 'ERROR');
  
  try {
    // Save error to properties for debugging
    PropertiesService.getScriptProperties().setProperty(
      'LAST_ERROR', 
      JSON.stringify(errorReport)
    );
  } catch (propError) {
    logWithTime(`Failed to save error report: ${propError.message}`, 'ERROR');
  }
  
  // Send immediate notification
  try {
    sendErrorNotification(error);
  } catch (emailError) {
    logWithTime(`Failed to send error notification: ${emailError.message}`, 'ERROR');
  }
  
  return errorReport;
}

// ===================================================================
// UTILITY AND DIAGNOSTIC FUNCTIONS
// ===================================================================
function debugDemographicsFile() {
  try {
    const incomingFolder = DriveApp.getFolderById(CONFIG.INCOMING_FOLDER_ID);
    const files = incomingFolder.getFiles();
    
    let demoFile = null;
    while (files.hasNext()) {
      const file = files.next();
      if (file.getName().includes('demographics')) {
        demoFile = file;
        break;
      }
    }
    
    if (!demoFile) {
      logWithTime('No demographics file found');
      return;
    }
    
    const content = demoFile.getBlob().getDataAsString('UTF-8');
    const lines = content.split('\n').map(line => line.trim()).filter(line => line.length > 0);
    
    logWithTime(`File: ${demoFile.getName()}`);
    logWithTime(`Total lines: ${lines.length}`);
    logWithTime('First 10 lines:');
    
    for (let i = 0; i < Math.min(10, lines.length); i++) {
      logWithTime(`Line ${i + 1}: ${lines[i]}`);
    }
    
    // Look for sections
    logWithTime('\nSearching for section patterns:');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.includes('Segment') || line.includes('segment')) {
        logWithTime(`Found potential section at line ${i + 1}: ${line}`);
      }
    }
    
  } catch (error) {
    logWithTime(`Debug failed: ${error.message}`, 'ERROR');
  }
}

function cleanupProcessedFiles(olderThanDays = 30) {
  try {
    logWithTime(`🧹 Cleaning up files older than ${olderThanDays} days`);
    
    const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
    const cutoffDate = new Date(Date.now() - (olderThanDays * 24 * 60 * 60 * 1000));
    
    const files = processedFolder.getFiles();
    let deletedCount = 0;
    
    while (files.hasNext()) {
      const file = files.next();
      if (file.getLastUpdated() < cutoffDate) {
        try {
          file.setTrashed(true);
          deletedCount++;
        } catch (deleteError) {
          logWithTime(`Failed to delete ${file.getName()}: ${deleteError.message}`, 'WARN');
        }
      }
    }
    
    logWithTime(`✅ Cleanup completed: ${deletedCount} files archived`);
    return { success: true, deleted: deletedCount };
    
  } catch (error) {
    logWithTime(`❌ Cleanup failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

function generateDiagnosticReport() {
  try {
    logWithTime('📊 Generating diagnostic report...');
    
    const report = {
      timestamp: new Date().toISOString(),
      system: getSystemStatus(),
      configuration: {
        totalSheets: 23,
        batchSize: CONFIG.BATCH_SIZE,
        maxProcessingTime: CONFIG.MAX_PROCESSING_TIME_MS / 1000,
        memoryCleanupInterval: CONFIG.MEMORY_CLEANUP_INTERVAL
      },
      filePatterns: Object.keys(FILE_PATTERNS).length,
      lastExecution: null
    };
    
    // Get last execution details
    try {
      const properties = PropertiesService.getScriptProperties();
      const lastResult = properties.getProperty('LAST_RESULT');
      if (lastResult) {
        report.lastExecution = JSON.parse(lastResult);
      }
    } catch (e) {
      report.lastExecution = { error: 'Could not retrieve last execution data' };
    }
    
    logWithTime('✅ Diagnostic report generated');
    return report;
    
  } catch (error) {
    logWithTime(`❌ Diagnostic report failed: ${error.message}`, 'ERROR');
    return { error: error.message };
  }
}

// ===================================================================
// MAIN EXECUTION WRAPPER
// ===================================================================

function main() {
  try {
    recordExecution({ timestamp: new Date().toISOString(), type: 'main_execution' });
    const result = processAllLinkedInAnalytics();
    recordExecution(result);
    return result;
  } catch (error) {
    const errorReport = handleCriticalError(error, 'main');
    recordExecution({ success: false, error: error.message });
    return { success: false, error: error.message, errorReport };
  }
}

// ===================================================================
// BATCH OPERATIONS (ADDITIONAL FEATURES)
// ===================================================================

function processSpecificFileTypes(fileTypes = []) {
  try {
    logWithTime(`🎯 Processing specific file types: ${fileTypes.join(', ')}`);
    
    const folders = validateFolders();
    if (!folders) throw new Error('Folder validation failed');
    
    const files = getFilesFromFolder(folders.incomingFolder);
    const filteredFiles = files.filter(file => {
      const fileName = file.getName();
      const fileType = detectFileType(fileName);
      return fileTypes.includes(fileType);
    });
    
    logWithTime(`Found ${filteredFiles.length} matching files`);
    
    if (filteredFiles.length === 0) {
      return { success: true, processed: 0, message: 'No matching files found' };
    }
    
    // Process filtered files using main logic
    processingState.startTime = Date.now();
    clearProcessingState();
    
    const results = { processed: 0, errors: 0 };
    
    filteredFiles.forEach((file, index) => {
      try {
        const fileName = file.getName();
        const fileType = detectFileType(fileName);
        const fileDate = extractDateFromFileName(fileName);
        
        logWithTime(`[${index + 1}/${filteredFiles.length}] Processing: ${fileName}`);
        
        let success = false;
        
        switch (fileType) {
          case 'campaign_performance':
            success = processCampaignFile(file, fileDate);
            break;
          case 'content_analytics':
            success = processContentFile(file, fileDate);
            break;
          case 'demographics_report':
            success = processDemographicsFile(file, fileDate);
            break;
          // Add other cases as needed
        }
        
        if (success) {
          file.moveTo(folders.processedFolder);
          results.processed++;
        } else {
          results.errors++;
        }
        
      } catch (error) {
        results.errors++;
        logWithTime(`Processing error: ${error.message}`, 'ERROR');
      }
    });
    
    const processingTime = (Date.now() - processingState.startTime) / 1000;
    clearProcessingState();
    
    logWithTime(`✅ Specific processing complete: ${results.processed} processed, ${results.errors} errors in ${processingTime}s`);
    
    return {
      success: true,
      processed: results.processed,
      errors: results.errors,
      time: processingTime
    };
    
  } catch (error) {
    logWithTime(`❌ Specific processing failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

function retryFailedFiles() {
  try {
    logWithTime('🔄 Retrying files that may have failed previously');
    
    const folders = validateFolders();
    if (!folders) throw new Error('Folder validation failed');
    
    const files = getFilesFromFolder(folders.incomingFolder);
    
    if (files.length === 0) {
      return { success: true, message: 'No files to retry' };
    }
    
    // Run standard processing
    return processAllLinkedInAnalytics();
    
  } catch (error) {
    logWithTime(`❌ Retry operation failed: ${error.message}`, 'ERROR');
    return { success: false, error: error.message };
  }
}

// ===================================================================
// EXPORT FUNCTIONS FOR TESTING AND MANUAL EXECUTION
// ===================================================================

// Main execution functions
var setupLinkedInAnalyticsAutomation = setupLinkedInAutomation;
var processAllLinkedInReports = processAllLinkedInAnalytics;
var runManualExecution = runLinkedInAutomation;
var processLinkedInFilesWorkingTemporal = processLinkedInFilesWorkingTemporal;
var processAllLinkedInReportsTemporal = processAllLinkedInAnalyticsTemporal; // ADD THIS LINE

// Testing and diagnostic functions
var testLinkedInAnalyticsAutomation = quickTest;
var testTemporalProcessing = testTemporalProcessing;
var runDiagnostics = generateDiagnosticReport;
var checkSystemHealth = getSystemStatus;

// Utility functions
var cleanupOldFiles = cleanupProcessedFiles;
var processSpecificFiles = processSpecificFileTypes;
var retryFiles = retryFailedFiles;

// Configuration functions
var updateSystemConfiguration = updateConfiguration;
var getSystemConfiguration = getSystemStatus;

// File processing functions (for manual testing)
var testFileDetection = detectFileType;
var validateFileFormat = validateFile;
var testCSVParsing = parseCSVSafely;

logWithTime('LinkedIn Analytics Automation System loaded successfully');
logWithTime('Available functions: setupLinkedInAnalyticsAutomation, processAllLinkedInReports, runManualExecution, testLinkedInAnalyticsAutomation');
var processAllLinkedInReports = processAllLinkedInReports;


// ===================================================================
// END OF SCRIPT
// ===================================================================
