use std::{ops::{Deref, DerefMut}, sync::{atomic::{AtomicUsize, Ordering}, RwLock, RwLockReadGuard, RwLockWriteGuard}};
use anyhow::{anyhow, Ok, Result};

// block engine 是 bptree 下面的一层抽象
// 有了这层抽象 bptree 的实现可以无需区分 disk / memory only

pub type BlockId = usize;

pub struct Block<B> {
    valid: bool,
    id: BlockId,
    content: Option<B>
}

pub trait BlockEngine {
    type Item;
    fn alloc_block(&mut self) -> BlockId;
    fn alloc_write(&mut self, item: Self::Item) -> Result<BlockId> {
        let id = self.alloc_block();
        let mut block = self.fetch_write(id)?;
        block.content = Some(item);
        block.valid = true;
        Ok(id)
    }
    fn fetch_read(&self, block_id: BlockId) -> Result<BlockReadGuard<Self::Item>>;
    fn fetch_write(&mut self, block_id: BlockId) -> Result<BlockWriteGuard<Self::Item>>;
    fn delete(&mut self, block_id: BlockId) -> Result<Option<Self::Item>>;
    
    // memory only 可以不实现
    // write back 不需要 engine 的内部状态
    fn write_back(block_id: BlockId, block: &Block<Self::Item>);
}

pub struct BlockReadGuard<'a, B> {
    rwlock_guard: RwLockReadGuard<'a, Block<B>>,
}

pub struct BlockWriteGuard<'a, B> {
    rwlock_guard: RwLockWriteGuard<'a, Block<B>>,
    write_back: fn(BlockId, &Block<B>) -> () 
}

pub struct MemoryBlockEngine<B> {
    // 纯内存存储下给每个 block 都上一把 rwlock 会不会开销太大？
    // disk 下内存中的 block cache 数量是固定的
    blocks: Vec<RwLock<Block<B>>>,
    next_block_id: AtomicUsize,
    free_list: Vec<BlockId>
}

impl <B> Deref for Block<B> {
    type Target = Option<B>;

    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl <B> DerefMut for Block<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.content
    }
}

impl <'a, B> Deref for BlockReadGuard<'a, B> {
    type Target = Block<B>;

    fn deref(&self) -> &Self::Target {
        self.rwlock_guard.deref()
    }
}

impl <'a, B> Deref for BlockWriteGuard<'a, B> {
    type Target = Block<B>;
    
    fn deref(&self) -> &Self::Target {
        self.rwlock_guard.deref()
    }
}

impl <'a, B> DerefMut for BlockWriteGuard<'a, B> {

    fn deref_mut(&mut self) -> &mut Self::Target {
        self.rwlock_guard.deref_mut()
    }
}

impl <'a, B> Drop for BlockWriteGuard<'a, B> {
    fn drop(&mut self) {
        let id = self.rwlock_guard.deref().id;
        (self.write_back)(id, self.deref())
    }
}

impl <B> BlockEngine for MemoryBlockEngine<B> {
    type Item = B;

    fn write_back(_block_id: BlockId, _block: &Block<B>) {
        // do nothing
    }
    
    fn alloc_block(&mut self) -> BlockId {
        let mut block_id: BlockId = 0;
        if !self.free_list.is_empty() {
            block_id = self.free_list.pop().unwrap()
        } else {
            block_id = self.next_block_id.fetch_add(1, Ordering::SeqCst);
            self.blocks.push(RwLock::new(Block { valid: false, content: None, id: block_id }));
        }
        // make it vaild
        self.blocks[block_id].write().unwrap().valid = true;
        block_id
    }
    
    fn fetch_read(&self, block_id: BlockId) -> Result<BlockReadGuard<Self::Item>> {
        if block_id >= self.next_block_id.load(Ordering::SeqCst) {
            return Err(anyhow!("invaild block id: {}.", block_id))
        }
        let anyhow::Result::Ok(read) = self.blocks[block_id].read() else {
            return Err(anyhow!("failed to aquire read lock."))
        };
        
        Ok(BlockReadGuard { rwlock_guard: read })
    }
    
    fn fetch_write(&mut self, block_id: BlockId) -> Result<BlockWriteGuard<Self::Item>> {
        if block_id >= self.next_block_id.load(Ordering::SeqCst) {
            return Err(anyhow!("invaild block id: {}.", block_id))
        }
        let anyhow::Result::Ok(write) = self.blocks[block_id].write() else {
            return Err(anyhow!("failed to aquire write lock."))
        };

        Ok(BlockWriteGuard { rwlock_guard: write, write_back: |block_id: BlockId, block: &Block<Self::Item>| Self::write_back(block_id, block) })
    }
    
    fn delete(&mut self, block_id: BlockId) -> Result<Option<Self::Item>> {
        if block_id >= self.next_block_id.load(Ordering::SeqCst) || self.free_list.contains(&block_id) {
            return Err(anyhow!("invaild block id: {}.", block_id))
        }
        self.free_list.push(block_id);
        Ok(self.blocks[block_id].write().unwrap().content.take())
    }
    
}

impl <B> MemoryBlockEngine<B> {
    pub fn new() -> Self {
        Self { blocks: vec![], next_block_id: AtomicUsize::new(0), free_list: vec![] }
    }
}
