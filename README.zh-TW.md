# 從第一原理理解開源專案

[English](README.md) | [繁體中文](README.zh-TW.md)

多數 onboarding 教材從 API 與術語開始。本 repository 先建立專案定位，再從物理限制、失敗模式、不變量與設計取捨出發，連到 source code、test、issue 與真實的貢獻流程。

目標是協助具備技術基礎的新手走向 **Contribution Ready**，而不只是學會 API。

## 學習路線

| Track | zh-TW | English | 狀態 |
| --- | --- | --- | --- |
| Kafka | [閱讀](docs/kafka/zh-TW/README.md) | [Read](docs/kafka/en/README.md) | 審閱用原型 |
| Ray | [閱讀](docs/ray/zh-TW/README.md) | [Read](docs/ray/en/README.md) | 審閱用原型 |
| YuniKorn | 規劃中 | 規劃中 | 規劃中 |

未來的 infrastructure tracks 可以沿用 `docs/` 下相同的語言與內容結構。

## 教學方法

```text
專案定位
→ 開場問題
→ 物理問題
→ 物理限制
→ 設計選擇
→ 機制
→ 保證
→ 不保證
→ 失敗邊界
→ Test / Source / Issue
→ 收束綜合
```

每一條 track 都維持簡短的學習主線，同時把有邊界的 guarantee 連到 contributor 所需的 upstream evidence。

**Layer 0 — 專案定位是所有現有與未來 track 的必要起點。** 在問「系統為什麼這樣設計？」之前，先用短篇幅說清楚：這是哪一類系統、解決什麼實際問題、擁有什麼決策權，以及不負責什麼。進入第一原理分析前，每條 track 都必須通過這項檢查：「新手能否在我們追問機制為何存在之前，先用幾句話解釋這個專案在做什麼？」

**每條 track 也必須讓開場問題（Opening Question）與收束綜合（Closing Synthesis）前後呼應。** Layer 0 之後先問：「如果不用這個專案，哪些工程負擔仍然存在？」主線結束後再回答：「理解機制之後，這個專案究竟替我們承擔了什麼責任？」先指出自行實作或其他系統仍須處理的工作，再把已檢視的機制連到它承擔的責任，以及使用者／維運者仍須處理的部分。未來 tracks 也必須遵守；這個框架不取代技術深度或失敗邊界。

## 方法論來源

本 repository 的教學方法，源自 **Yen-Hua Chen** 在 **Streaming System + Compass** 中長期記錄的工程推理方式。

這套方法最早透過 Kafka track 具體發展；Ray track 接著沿用相同流程：

1. 建立 Compass-derived reasoning profile；
2. 獨立驗證每個 project 的實際機制；
3. 排除物理機制不相符的錯誤類比；
4. 把 physical problems 整理成有邊界的 guarantees 與 non-guarantees；
5. 再連到該 project 的文件、source、tests 與 issues。

本 repository 正在測試：同一套 first-principles onboarding 方法，能否在獨立驗證各 project 實際機制的前提下，跨不同 infrastructure projects 重複使用。這並不是在主張 Kafka 或 Ray 的 architecture 源自 Compass。

方法論原始來源：[https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)

## 授權

文件與教育性文字採用 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)；軟體與具實質內容、可獨立執行的 examples 採用 [Apache License 2.0](LICENSE)。文件內的簡短示意程式碼仍是該 CC BY 4.0 文件的一部分。

完整授權範圍請見 [LICENSE-CONTENT.md](LICENSE-CONTENT.md)。

## 語言

本頁是 `zh-TW` 繁體中文首頁。英文首頁請見 [README.md](README.md)。
