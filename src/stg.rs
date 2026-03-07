pub use atom_file::*;

/// Interface for page storage.
pub trait PageStorage: Send + Sync {
    /// Is the underlying storage new?
    fn is_new(&self) -> bool;
    /// Information about page sizes.
    fn info(&self) -> Box<dyn PageStorageInfo>;
    /// Make a new page, result is page number.
    fn new_page(&mut self) -> u64;
    /// Drop page number.
    fn drop_page(&mut self, pn: u64);
    /// Set contents of page.
    fn set_page(&mut self, pn: u64, data: Data);
    /// Get contents of page.
    fn get_page(&self, pn: u64) -> Data;
    /// Get page size (for repacking).
    fn size(&self, pn: u64) -> usize;
    /// Save pages to underlying storage.
    fn save(&mut self);
    /// Undo changes since last save ( but set_page/renumber cannot be undone, only new_page and drop_page can be undone ).
    fn rollback(&mut self);
    /// Wait until save is complete.
    fn wait_complete(&self);
    #[cfg(feature = "verify")]
    /// Get set of free pages and number of pages ever allocated ( for VERIFY builtin function ).
    fn get_free(&mut self) -> (crate::HashSet<u64>, u64);
    #[cfg(feature = "renumber")]
    /// Renumber page.
    fn renumber(&mut self, pn: u64) -> u64;
    #[cfg(feature = "renumber")]
    /// Load free pages in preparation for page renumbering. Returns number of used pages or None if there are no free pages.
    fn load_free_pages(&mut self) -> Option<u64>;
    #[cfg(feature = "renumber")]
    /// Final part of page renumber operation.
    fn set_alloc_pn(&mut self, target: u64);
}

/// Information about page sizes.
pub trait PageStorageInfo: Send + Sync {
    /// Number of different page sizes.
    fn sizes(&self) -> usize;
    /// Size index for given page size.
    fn index(&self, size: usize) -> usize;
    /// Page size for ix ( 1-based ix must be <= sizes() ).
    fn size(&self, ix: usize) -> usize;
    /// Maximum size page.
    fn max_size_page(&self) -> usize {
        self.size(self.sizes())
    }
    /// Half size page.
    fn half_size_page(&self) -> usize {
        self.size(self.index(self.max_size_page() / 2 - 50))
    }
    /// Is it worth compressing a page of given size by saving.
    fn compress(&self, size: usize, saving: usize) -> bool {
        self.index(size - saving) < self.index(size)
    }
}
