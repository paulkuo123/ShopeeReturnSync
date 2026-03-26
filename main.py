import pandas as pd
import os
from glob import glob

# ================== 設定區 ==================
主庫存資料夾 = "主庫存"
退貨資料夾 = "退貨單"

# 欄位名稱（請確認與你的檔案完全一致）
退貨_商品ID欄 = "商品ID"
退貨_規格ID欄 = "規格ID"
退貨_數量欄 = "數量"

主_商品ID欄 = "et_title_product_id"
主_選項ID欄 = "et_title_variation_id"
主_庫存欄 = "et_title_variation_stock"
# ===========================================

print("=== 蝦皮退貨補庫存工具開始執行 ===\n")

# 1. 讀取所有退貨報表
print("正在讀取所有退貨報表...")
all_returns = pd.DataFrame()

退貨檔案列表 = glob(os.path.join(退貨資料夾, "*.xlsx")) + glob(os.path.join(退貨資料夾, "*.xls"))

for f in 退貨檔案列表:
    檔名 = os.path.basename(f)
    if 檔名.startswith('~$'):
        print(f"⏭️  跳過暫存檔: {檔名}")
        continue
    try:
        df = pd.read_excel(f, engine='openpyxl')
        all_returns = pd.concat([all_returns, df], ignore_index=True)
        print(f"✅ 已讀取退貨報表: {檔名}  ({len(df)} 筆)")
    except Exception as e:
        print(f"❌ 讀取退貨報表失敗 {檔名}: {str(e)[:120]}")

if all_returns.empty:
    print("\n❌ 沒有讀取到任何退貨資料！")
    print("請確認退貨單資料夾內已有「不加密」的 .xlsx 檔案。")
    exit()

# 確保 ID 欄位是字串且去除空格
all_returns[退貨_商品ID欄] = all_returns[退貨_商品ID欄].astype(str).str.strip()
all_returns[退貨_規格ID欄] = all_returns[退貨_規格ID欄].astype(str).str.strip()

returns_sum = all_returns.groupby([退貨_商品ID欄, 退貨_規格ID欄])[退貨_數量欄].sum().reset_index()
print(f"\n✅ 退貨總共 {len(returns_sum)} 種規格需要補貨\n")

# 2. 處理主庫存檔案
主庫存檔案列表 = glob(os.path.join(主庫存資料夾, "*.xlsx")) + glob(os.path.join(主庫存資料夾, "*.xls"))

for 主檔路徑 in 主庫存檔案列表:
    檔名 = os.path.basename(主檔路徑)
    if 檔名.startswith('~$') or 檔名.startswith('已補退貨_'):
        continue

    print(f"正在處理主庫存檔案: {檔名}")

    try:
        # 優先嘗試 calamine 引擎，它比較快且不容易出錯
        main = pd.read_excel(主檔路徑, engine='calamine')
    except Exception:
        try:
            # openpyxl 的 data_only=True 可以讀取公式計算後的結果
            main = pd.read_excel(主檔路徑, engine='openpyxl', data_only=True)
        except Exception as e:
            print(f"❌ 無法讀取主庫存檔案 {檔名}: {str(e)[:100]}")
            continue

    # 檢查必要欄位是否存在
    if 主_商品ID欄 not in main.columns or 主_選項ID欄 not in main.columns or 主_庫存欄 not in main.columns:
        print(f"❌ 檔案 {檔名} 缺少必要欄位，請檢查欄位設定！")
        print(f"   當前欄位: {main.columns.tolist()[:10]}...")
        continue

    # 確保 ID 欄位都是字串格式，避免型別不一致導致合併失敗
    main[主_商品ID欄] = main[主_商品ID欄].astype(str).str.strip()
    main[主_選項ID欄] = main[主_選項ID欄].astype(str).str.strip()

    # 只對有商品 ID 的列進行庫存加總，避免破壞檔案頭部的 metadata
    # 我們識別資料列的方式是：主_商品ID欄 必須是純數字字串
    is_data_row = main[主_商品ID欄].str.match(r'^\d+$', na=False)

    # 合併 + 找出找不到的商品
    merged = main.merge(
        returns_sum,
        left_on=[主_商品ID欄, 主_選項ID欄],
        right_on=[退貨_商品ID欄, 退貨_規格ID欄],
        how='left'
    )

    # 找出在 returns_sum 中有但 main 中沒找到的商品 (排除 metadata 列)
    找不到的退貨 = merged[merged[主_庫存欄].isna() & merged[退貨_數量欄].notna() & is_data_row]
    if not 找不到的退貨.empty:
        print(f"⚠️  提醒：以下 {len(找不到的退貨)} 筆退貨商品在主庫存中找不到（可能已下架）：")
        for _, row in 找不到的退貨.iterrows():
            print(f"   → 商品ID: {row[退貨_商品ID欄]} | 規格ID: {row[退貨_規格ID欄]} | 退貨數量: {int(row[退貨_數量欄])}")
        print("")

    # 加回庫存，只針對資料列處理
    # 建立一個輔助 Series 用於計算新庫存
    current_stock = pd.to_numeric(merged.loc[is_data_row, 主_庫存欄], errors='coerce').fillna(0).astype(int)
    return_qty = merged.loc[is_data_row, 退貨_數量欄].fillna(0).astype(int)
    
    # 更新庫存
    merged.loc[is_data_row, 主_庫存欄] = current_stock + return_qty

    final = merged[main.columns].copy()

    輸出路徑 = os.path.join(主庫存資料夾, f"已補退貨_{檔名}")
    final.to_excel(輸出路徑, index=False, engine='openpyxl')

    print(f"✅ 已輸出: 已補退貨_{檔名}  （總共處理 {len(final)} 筆商品）\n")

print("🎉 全部處理完成！")
print("請到「主庫存」資料夾，把「已補退貨_」開頭的檔案上傳到蝦皮批次更新即可。")