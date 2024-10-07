use anyhow::{Ok, Result};
use std::{cell::Cell, fmt::Debug, marker::PhantomData};

use crate::block::{BlockEngine, BlockId};

pub struct BPlusTree<K, V, E>
where
    E: BlockEngine<Item = BPlusTreeNode<K, V>>,
    K: Ord,
{
    way: usize,
    engine: E,
    root: BlockId,
    _marker1: PhantomData<K>,
    _marker2: PhantomData<V>,
}

pub struct BPlusTreeNode<K: Ord, V> {
    parent: Cell<Option<BlockId>>,
    way: usize,
    is_leaf: bool,
    // sorted
    keys: Vec<K>,
    // leaf only
    values: Vec<V>,
    prev: Option<BlockId>,
    next: Option<BlockId>,

    // inner only
    pointers: Vec<BlockId>,
}

impl<K: Ord, V> BPlusTreeNode<K, V> {
    fn is_leaf(&self) -> bool {
        self.is_leaf
    }

    fn new_leaf(way: usize, parent: Option<usize>) -> BPlusTreeNode<K, V> {
        BPlusTreeNode {
            parent: Cell::new(parent),
            way,
            is_leaf: true,
            keys: vec![],
            values: vec![],
            prev: None,
            next: None,
            pointers: vec![],
        }
    }

    fn new_inner(way: usize) -> BPlusTreeNode<K, V> {
        BPlusTreeNode {
            parent: Cell::new(None),
            way,
            is_leaf: false,
            keys: vec![],
            values: vec![],
            prev: None,
            next: None,
            pointers: vec![],
        }
    }
}

impl<K, V, E> BPlusTree<K, V, E>
where
    E: BlockEngine<Item = BPlusTreeNode<K, V>>,
    K: Ord + Clone,
    V: Clone,
{

    pub fn new(way: usize, mut engine: E) -> BPlusTree<K, V, E> {
        let root = engine.alloc_write(BPlusTreeNode::new_leaf(way, None)).unwrap();
        BPlusTree {
            way,
            engine,
            root,
            _marker1: PhantomData,
            _marker2: PhantomData,
        }
    }

    pub fn search(&self, key: &K) -> Option<V> {
        self.search_helper(self.root, key)
    }

    fn search_helper(&self, block_id: BlockId, key: &K) -> Option<V> {
        let read = self.engine.fetch_read(block_id).unwrap();
        if read.is_none() {
            return None;
        }
        let BPlusTreeNode {
            parent: _,
            way: _,
            is_leaf,
            keys,
            values,
            prev: _,
            next: _,
            pointers,
        } = read.as_ref().unwrap();

        if !*is_leaf {
            let pos = keys
                    .binary_search(key)
                    .unwrap_or_else(|e| e);
            self.search_helper(pointers[if pos < keys.len() && *key == keys[pos] { pos + 1 } else { pos }], key)
        } else {
            keys.binary_search(key).ok().map(|index| values[index].clone())
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        
        let parent = Cell::new(None);
        // 找到正确的子结点
        Self::insert_helper(&mut self.engine, &parent, self.root, key, value)?;
        if parent.get().is_some() {
            self.root = parent.get().unwrap()
        }

        Ok(())
    }

    fn insert_helper(
        engine: *mut E,
        parent: &Cell<Option<BlockId>>,
        block_id: BlockId,
        key: K,
        value: V,
    ) -> Result<()> {
        let mut guard = unsafe { engine.as_mut().unwrap() }.fetch_write(block_id).unwrap(); 
        if guard.is_none() {
            return Ok(());
        }
        let node = guard.as_mut().unwrap();
        if node.is_leaf {
            let pos = node.keys.binary_search(&key).unwrap_or_else(|e| e);
            node.keys.insert(pos, key);
            node.values.insert(pos, value);
        } else {
            let pos = node.keys
                .binary_search(&key)
                .unwrap_or_else(|e| e);
            let child = node.pointers[pos];
            Self::insert_helper(engine, &node.parent, child, key, value)?;
        }

        if node.keys.len() > node.way {
            if node.is_leaf {
                let right_keys = node.keys.split_off(node.keys.len() / 2);
                let right_values = node.values.split_off(node.values.len() / 2);
                let mid = right_keys[0].clone();
                let way = node.way;
                if parent.get().is_none() {
                    let mut node = BPlusTreeNode::new_inner(way);
                    node.pointers =  vec![block_id];
                    parent.set(unsafe { engine.as_mut().unwrap() }.alloc_write(node).ok());
                    assert_ne!(parent.get(), None, "alloc write failed.")
                }
                let mut parent_block = unsafe { engine.as_mut().unwrap() }
                    .fetch_write(parent.get().unwrap())?;
                let parent_block_ref = parent_block.as_mut().unwrap();
                let pos = parent_block_ref
                    .keys
                    .binary_search(&mid)
                    .unwrap_or_else(|e| e);
                
                let right_block_id = unsafe { engine.as_mut().unwrap() }.alloc_write(
                    BPlusTreeNode { 
                        parent: parent.clone(), 
                        way, 
                        is_leaf: true,
                        keys: right_keys,
                        values: right_values,
                        prev: Some(block_id),
                        next: node.next,
                        pointers: vec![]
                    }
                )?;
                parent_block_ref.keys.insert(pos, mid);
                parent_block_ref.pointers.insert(pos + 1, right_block_id);
                node.next = Some(right_block_id);
            } else {
                let mut right_keys = node.keys.split_off(node.keys.len() / 2);
                let right_pointers = node.pointers.split_off(node.keys.len() / 2);
                let mid = right_keys.remove(0);
                if parent.get().is_none() {
                    parent.set(unsafe { engine.as_mut().unwrap() }.alloc_write(BPlusTreeNode::new_inner(node.way)).ok());
                    assert_ne!(parent.get(), None, "alloc write failed.")
                }
                let mut parent_block = unsafe { engine.as_mut().unwrap() }
                    .fetch_write(parent.get().unwrap())?;
                let parent_block_ref = parent_block.as_mut().unwrap();
                let pos = parent_block_ref
                    .keys
                    .binary_search(&mid)
                    .unwrap_or_else(|e| e);
                let right_block_id = unsafe { engine.as_mut().unwrap() }.alloc_write(
                    BPlusTreeNode { 
                        parent: parent.clone(), 
                        way: node.way, 
                        is_leaf: false,
                        keys: right_keys,
                        values: vec![],
                        prev: Some(block_id),
                        next: node.next,
                        pointers: right_pointers
                    }
                )?;
                parent_block_ref.keys.insert(pos, mid);
                parent_block_ref.pointers.insert(pos + 1, right_block_id);
            }
        }

        Ok(())
    }

    // todo: delete 
    // 懒得实现了
    pub fn delete(&mut self, key: &K) -> Result<Option<V>> {
        let parent = Cell::new(None);
        // 找到正确的子结点
        let ret = Self::delete_helper(&mut self.engine, &parent, self.root, key)?;
        if parent.get().is_some() {
            self.root = parent.get().unwrap()
        }
        Ok(ret)
    }

    fn delete_helper(engine: *mut E, parent: &Cell<Option<BlockId>>, block_id: BlockId, key: &K) -> Result<Option<V>> {
        let mut guard = unsafe { engine.as_mut().unwrap() }.fetch_write(block_id).unwrap();
        let mut ret: Option<V> = None;
        if guard.is_none() {
            return Ok(None);
        }
        let node = guard.as_mut().unwrap();
        if node.is_leaf {
            let Result::Ok(pos) = node.keys.binary_search(key) else {
                return Ok(None)
            };
            node.keys.remove(pos);
            ret = Some(node.values.remove(pos));
        } else {
            let Result::Ok(pos) = node.keys.binary_search(key) else {
                return Ok(None)
            };
            let child = node.pointers[pos];
            ret = Self::delete_helper(engine, &node.parent, child, key)?;
        }

        // if node.is_leaf && node.keys.is_empty() {
            
        // }



        Ok(ret)
    }

    pub fn print_tree(&self) where K : Debug, V : Debug {
        self.print_tree_helper(self.root, 0);
    }

    fn print_tree_helper(&self, block_id: BlockId, depth: usize) where K : Debug, V : Debug {
        if let Some(node) = self.engine.fetch_read(block_id).unwrap().as_ref() {
            let indent = " ".repeat(depth * 2);
            if node.is_leaf {
                println!("{}Leaf: {:?} values: {:?}", indent, node.keys, node.values);
            } else {
                println!("{}Inner: {:?} values: {:?}", indent, node.keys, node.values);
                for &child_id in &node.pointers {
                    self.print_tree_helper(child_id, depth + 1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::MemoryBlockEngine;

    use super::*;

    #[test]
    fn test_insert_and_search() {
        let way = 2;
        let engine = MemoryBlockEngine::new();
        let mut tree = BPlusTree::new(way, engine);

        // Test insert
        tree.insert(1, "apple".to_string()).unwrap();
        tree.insert(2, "banana".to_string()).unwrap();
        tree.insert(3, "cherry".to_string()).unwrap();

        // Inner: [2]
        //   Leaf: [1]
        //   Leaf: [2, 3]
        // 结果可以在 https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html 验证
        tree.print_tree();

        // Test search
        assert_eq!(tree.search(&1), Some("apple".into()));
        assert_eq!(tree.search(&2), Some("banana".into()));
        assert_eq!(tree.search(&3), Some("cherry".into()));
        assert_eq!(tree.search(&4), None); // Key not present
    }
}