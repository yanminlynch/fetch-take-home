Hey team,

I’ve been reviewing the Fetch Rewards data and ran into a few data quality issues that are affecting our analysis. Below are the key findings, some questions that need clarification, and suggested next steps.

---

### **Key Issues & Findings**

* **User Data Seems Outdated**  
   The most recent user creation date in our dataset is from February 2021, meaning there are no users from the past six months. Because of this, any analysis involving new users (like spending trends) is returning no results.  
   **Question:** Is this an expected limitation of the dataset, or is there a more updated source for user creation data?

* **Inconsistent Date Formats**  
   Some date fields, like `purchaseDate_$date` and `createdDate_$date`, were stored as Unix timestamps in milliseconds instead of standard date formats. This caused issues when trying to filter transactions by month. The data has been converted to `YYYY-MM-DD` for now.  
   **Suggestion:** Should we standardize this at ingestion to prevent future issues?

* **Brand Code Mismatches**  
   Some `brandCode` values in the receipts dataset do not match any brand in the brands dataset, which is leading to missing data in reports that analyze brand performance.  
   **Question:** Do we have a brand mapping table, or should we build logic to handle missing brands?

* **Receipt Status Differences**  
   The dataset does not contain an `ACCEPTED` status. Instead, the available statuses are `FINISHED`, `REJECTED`, and `FLAGGED`. We've adjusted the queries to compare `FINISHED` vs. `REJECTED`, assuming `FINISHED` means a successful transaction.  
   **Question:** Can we confirm that `FINISHED` is the correct equivalent of `ACCEPTED`? Should `FLAGGED` receipts be included in any of our reports?

---

### **What I Need Help With**

* Can someone confirm if the missing user data is due to an ETL delay or if we need access to a different data source?  
* Is there an official brand mapping table we can use to fix mismatched brand codes?  
* Should `FLAGGED` receipts be treated differently in our reporting?

---

### **Next Steps**

* Look into the missing user data issue to determine if this is an ETL delay or a data access issue.  
* Consider standardizing timestamps at ingestion so they don't need conversion later.  
* Validate brand code mappings and decide how to handle missing brands.  
* Confirm the meaning of `FINISHED` receipts and clarify whether `FLAGGED` receipts should be included in analyses.

Let me know your thoughts, and we can discuss this further in our next sync.

Thanks,  
Yanmin 

