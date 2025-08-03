use std::io::{
    self,
    Write,
};
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::process;
use std::env;
use std::io::{Seek, SeekFrom, Read};
use std::mem::size_of;
use std::convert::TryInto;

use scan_fmt::scan_fmt;
use memoffset::offset_of;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

const ID_SIZE: usize = size_of::<u32>();
const USERNAME_SIZE: usize = COLUMN_USERNAME_SIZE;
const EMAIL_SIZE: usize = COLUMN_EMAIL_SIZE;

const ID_OFFSET: usize = offset_of!(Row, id);
const USERNAME_OFFSET: usize = offset_of!(Row, username);
const EMAIL_OFFSET: usize = offset_of!(Row, email);

const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

/// const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
// const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

/* Common Header Layout */ 
const NODE_TYPE_SIZE: usize = size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;

const IS_ROOT_SIZE: usize = size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;

const PARENT_POINTER_SIZE: usize = size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;

const COMMON_NODE_HEADER_SIZE: usize =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
const INTERNAL_NODE_KEY_OFFSET: usize = INTERNAL_NODE_CHILD_SIZE;
/* Leaf Node Header Layout */
const LEAF_NODE_NUM_CELLS_SIZE: usize = size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;

/* Leaf Node Body Layout */
const LEAF_NODE_KEY_SIZE: usize = size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;

const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// Leaf node split balancing constants
const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/* Internal Node Header Layout */
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE +
    INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/* Internal Node Body Layout */
const INTERNAL_NODE_KEY_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_CHILD_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

// Leaf node header layout
pub const LEAF_NODE_NEXT_LEAF_SIZE: usize = size_of::<u32>();
pub const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

const INVALID_PAGE_NUM: u32 = u32::MAX;

const INTERNAL_NODE_MAX_CELLS: usize = 3; 


/* Example helper function */
fn get_u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn get_u32_at_mut(data: &mut [u8], offset: usize) -> &mut u32 {
    let ptr = data[offset..offset + 4].as_mut_ptr() as *mut u32;
    unsafe { &mut *ptr }
}

fn leaf_node_next_leaf(node: &[u8]) -> u32 {
    let mut buf = [0u8; 4]; // Temporary buffer to hold 4 bytes
    buf.copy_from_slice(&node[LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + 4]);
    u32::from_le_bytes(buf)
}

/* Internal Node Read/Write Accessors */
pub fn internal_node_num_keys(node: &mut [u8]) -> &mut u32 {
    get_u32_at_mut(node, INTERNAL_NODE_NUM_KEYS_OFFSET)
}
pub fn internal_node_right_child(node: &mut [u8]) -> &mut u32 {
    get_u32_at_mut(node, INTERNAL_NODE_RIGHT_CHILD_OFFSET)
}
pub fn internal_node_cell_offset(cell_num: usize) -> usize {
    INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE
}

pub fn internal_node_child(node: &mut [u8], child_num: usize) -> &mut u32 {
    let num_keys = *internal_node_num_keys(node);

    if child_num > num_keys as usize {
        println!(
            "Tried to access child_num {} > num_keys {}",
            child_num, num_keys
        );
        process::exit(1);
    } else if child_num == num_keys as usize {
        let right_child = internal_node_right_child(node);
        if *right_child == INVALID_PAGE_NUM {
            println!("Tried to access right child of node, but was invalid page number");
            process::exit(1);
        }
        return right_child;
    } else {
        let offset = internal_node_cell_offset(child_num);
        let child_ptr = get_u32_at_mut(node, offset);
        if *child_ptr == INVALID_PAGE_NUM {
            println!(
                "Tried to access child {} of node, but was invalid page number",
                child_num
            );
            process::exit(1);
        }
        return child_ptr;
    }
}

fn internal_node_key_at(node: &[u8], key_num: usize) -> u32 {
    let offset = internal_node_cell_offset(key_num) + INTERNAL_NODE_CHILD_SIZE;
    get_u32_at(node, offset)
}

pub fn internal_node_key(node: &mut [u8], key_num: usize) -> &mut u32 {
    let offset = internal_node_cell_offset(key_num) + INTERNAL_NODE_CHILD_SIZE;
    get_u32_at_mut(node, offset)
}

fn get_node_max_key(pager: &mut Pager, page_num: usize) -> u32 {
    let node = get_page(pager, page_num).expect("Failed to get page");
    
    match get_node_type(node) {
        NodeType::Leaf => {
            // Get number of cells (i.e., key-value pairs)
            let num_cells = leaf_node_num_cells(node);
            // Return the last key in the leaf node
            leaf_node_key(node, (num_cells - 1) as usize)
        }
        NodeType::Internal => {
            // Follow the rightmost child recursively
            let right_child_page_num = *internal_node_right_child(node) as usize;
            get_node_max_key(pager, right_child_page_num)
        }
    }
}


fn internal_node_cell_mut(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let offset = internal_node_cell_offset(cell_num);
    &mut node[offset..offset + INTERNAL_NODE_CELL_SIZE]
}

fn internal_node_cell(node: &[u8], cell_num: usize) -> &[u8] {
    let offset = internal_node_cell_offset(cell_num);
    &node[offset..offset + INTERNAL_NODE_CELL_SIZE]
}

// Helper to set internal node child at specific index
fn set_internal_node_child(node: &mut [u8], child_num: usize, page_num: u32) {
    *internal_node_child(node, child_num) = page_num;
}

// Helper to set the right child (using your existing function)
fn set_internal_node_right_child(node: &mut [u8], page_num: u32) {
    *internal_node_right_child(node) = page_num;
}


//Keep track of the root node
fn is_node_root(node: &[u8]) -> bool {
    node[IS_ROOT_OFFSET] != 0
}

fn set_node_root(node: &mut [u8], is_root: bool) {
    node[IS_ROOT_OFFSET] = is_root as u8;
}



#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

// read the number of cells in a leaf node
fn leaf_node_num_cells(node: &[u8]) -> u32 {
    let start = LEAF_NODE_NUM_CELLS_OFFSET;
    let end = start + 4;
    u32::from_le_bytes(node[start..end].try_into().unwrap())
}

// set the number of cells in a leaf node
fn set_leaf_node_num_cells(node: &mut [u8], num_cells: u32) {
    let bytes = num_cells.to_le_bytes();
    let start = LEAF_NODE_NUM_CELLS_OFFSET;
    node[start..start + 4].copy_from_slice(&bytes);
}

// get the offset of the n-th cell in a leaf node
fn leaf_node_cell_offset(cell_num: usize) -> usize {
    LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE  
}

// get a slice of the n-th cell
fn leaf_node_key(node: &[u8], cell_num: usize) -> u32 {
    let offset = leaf_node_cell_offset(cell_num);
    u32::from_le_bytes(node[offset..offset + 4].try_into().unwrap())
}

/// get a slice of the value in the n-th cell
fn leaf_node_value(node: &[u8], cell_num: usize) -> &[u8] {
    let offset = leaf_node_cell_offset(cell_num) + LEAF_NODE_KEY_SIZE;
    &node[offset..offset + ROW_SIZE]
}

// Added: Helper function to get mutable slice of value in a cell
fn leaf_node_value_mut(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let offset = leaf_node_cell_offset(cell_num) + LEAF_NODE_KEY_SIZE;
    &mut node[offset..offset + ROW_SIZE]
}

// Added: Helper function to get mutable slice of a cell
fn leaf_node_cell(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let offset = leaf_node_cell_offset(cell_num);
    &mut node[offset..offset + LEAF_NODE_CELL_SIZE]
}

fn get_page_mut(pager: &mut Pager, page_num: usize) -> Option<&mut [u8; PAGE_SIZE]> {
    get_page(pager, page_num)
}

/// Initialize a new leaf node (set num_cells = 0)
fn initialize_leaf_node(node: &mut [u8]) {
    set_node_type(node, NodeType::Leaf);
    set_node_root(node, false);
    set_leaf_node_num_cells(node, 0);
    set_leaf_node_next_leaf(node, 0);
}

fn initialize_internal_node(node: &mut [u8]) {
    set_node_type(node, NodeType::Internal);
    set_node_root(node, false);
    set_internal_node_num_keys(node, 0);
    set_internal_node_right_child(node, INVALID_PAGE_NUM);
}

fn set_internal_node_num_keys(node: &mut [u8], value: u32) {
    let bytes = value.to_le_bytes();
    node[INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + 4]
        .copy_from_slice(&bytes);
}

fn get_leaf_node_next_leaf(node: &[u8]) -> u32 {
    let bytes: [u8; 4] = node[LEAF_NODE_NEXT_LEAF_OFFSET as usize..(LEAF_NODE_NEXT_LEAF_OFFSET + 4) as usize]
        .try_into()
        .unwrap();
    u32::from_le_bytes(bytes)
}

fn set_leaf_node_next_leaf(node: &mut [u8], next_leaf: u32) {
    let bytes = next_leaf.to_le_bytes();
    node[LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + 4]
        .copy_from_slice(&bytes);
}

fn update_internal_node_key(node: &mut [u8], old_key: u32, new_key: u32) {
    let child_index = internal_node_find_child(node, old_key);
    set_internal_node_key(node, child_index as usize, new_key);
}

fn set_internal_node_key(node: &mut [u8], index: usize, key: u32) {
    let offset = INTERNAL_NODE_HEADER_SIZE + index * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_KEY_OFFSET;
    node[offset..offset + 4].copy_from_slice(&key.to_le_bytes());
}


struct Pager {
    file_descriptor: File,
    file_length: u64,  // Changed from usize to u64
    num_pages: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES],
}

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_num: usize,
    cell_num: usize,
    end_of_table: bool,
}

fn table_start(table: &mut Table) -> Cursor {
    let page_num = 0;
    let node = get_page(&mut table.pager, page_num)
        .expect("Failed to load page in table_start");

    let num_cells = leaf_node_num_cells(node);

    Cursor {
        table,
        page_num,
        cell_num: 0,
        end_of_table: num_cells == 0,
    }
}


fn table_find(table: &mut Table, key: usize) -> Cursor {
    let root_page_num = table.root_page_num;
    let root_node = get_page(&mut table.pager, root_page_num)
        .expect("Failed to get root node"); 

    if get_node_type(root_node) == NodeType::Leaf {
        leaf_node_find(table, root_page_num, key as u32)
    } else {
        return internal_node_find(table, root_page_num, key);
    }
}

fn internal_node_find_child(node: &[u8], key: u32) -> u32 {
    let num_keys = get_u32_at(node, INTERNAL_NODE_NUM_KEYS_OFFSET);

    // Binary search
    let mut left = 0u32;
    let mut right = num_keys;

    while left != right {
        let mid = (left + right) / 2;
        let mid_key = internal_node_key_at(node, mid as usize);

        if key <= mid_key {
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    left
}

fn internal_node_find(table: &mut Table, page_num: usize, key: usize) -> Cursor {
    // Get the internal node from the page
    let node = get_page(&mut table.pager, page_num).expect("Failed to get node");
    
    let child_index = internal_node_find_child(node, key as u32); // Convert key to u32
    let child_page_num = *internal_node_child(node, child_index as usize) as usize; // Convert child_index to usize
    let child = get_page(&mut table.pager, child_page_num).expect("Failed to get child node");

    // Recurse or return cursor depending on child type
    match get_node_type(child) {
        NodeType::Leaf => leaf_node_find(table, child_page_num, key as u32),
        NodeType::Internal => internal_node_find(table, child_page_num, key),
    }
}

fn internal_node_insert(table: &mut Table, parent_page_num: usize, child_page_num: usize) {
    // Step 1: Compute child_max_key first
    let child_max_key = get_node_max_key(&mut table.pager, child_page_num);

    // Step 2: Get parent info and check capacity
    let (original_num_keys, right_child_page_num) = {
        let parent = get_page(&mut table.pager, parent_page_num).expect("Failed to get parent");
        let num_keys = *internal_node_num_keys(parent);
        let right_child = *internal_node_right_child(parent);
        (num_keys, right_child)
    };

    // Step 3: Handle max capacity case
    if original_num_keys >= INTERNAL_NODE_MAX_CELLS as u32 {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    // Step 4: Handle case where right child is invalid
    if right_child_page_num == INVALID_PAGE_NUM {
        let parent = get_page(&mut table.pager, parent_page_num).expect("Failed to get parent");
        *internal_node_right_child(parent) = child_page_num as u32;
        return;
    }

    // Step 5: Get the index where we should insert
    let index = {
        let parent = get_page(&mut table.pager, parent_page_num).expect("Failed to get parent");
        internal_node_find_child(parent, child_max_key) as usize
    };

    // Step 6: Get right_max_key
    let right_max_key = get_node_max_key(&mut table.pager, right_child_page_num as usize);

    // Step 7: Perform the insertion
    {
        let parent = get_page(&mut table.pager, parent_page_num).expect("Failed to get parent");
        
        if child_max_key > right_max_key {
            // Insert at the end and move right child
            *internal_node_child(parent, original_num_keys as usize) = right_child_page_num;
            *internal_node_key(parent, original_num_keys as usize) = right_max_key;
            *internal_node_right_child(parent) = child_page_num as u32;
        } else {
            // Shift existing cells and insert in the middle
            for i in (index..original_num_keys as usize).rev() {
                // We need to be careful about borrowing here
                let cell_data = internal_node_cell(parent, i).to_vec();
                let dest_cell = internal_node_cell_mut(parent, i + 1);
                dest_cell.copy_from_slice(&cell_data);
            }

            *internal_node_child(parent, index) = child_page_num as u32;
            *internal_node_key(parent, index) = child_max_key;
        }

        *internal_node_num_keys(parent) = original_num_keys + 1;
    }
}


fn leaf_node_find(table: &mut Table, page_num: usize, key: u32) -> Cursor {
    // Scope the node fetch so the borrow ends
    let num_cells;
    {
        let node = get_page(&mut table.pager, page_num)
            .expect("Failed to get node");
        num_cells = leaf_node_num_cells(node);
    }

    // Now it's safe to use table again
    let mut cursor = Cursor {
        table,
        page_num,
        cell_num: 0,
        end_of_table: false,
    };

    // Binary search
    let mut min_index = 0;
    let mut one_past_max_index = num_cells;

    // To access the node again, re-borrow
    let node = get_page(&mut cursor.table.pager, page_num)
        .expect("Failed to get node again");

    while min_index != one_past_max_index {
        let index = (min_index + one_past_max_index) / 2;
        let key_at_index = leaf_node_key(node, index as usize);

        if key == key_at_index {
            cursor.cell_num = index as usize;
            return cursor;
        } else if key < key_at_index {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor.cell_num = min_index as usize;
    cursor
}


// Get node type from a byte slice (read-only)
fn get_node_type(node: &[u8]) -> NodeType {
    match node[NODE_TYPE_OFFSET] {
        0 => NodeType::Internal,
        1 => NodeType::Leaf,
        _ => panic!("Unknown node type"),
    }
}

// Set node type in a mutable byte slice
fn set_node_type(node: &mut [u8], node_type: NodeType) {
    node[NODE_TYPE_OFFSET] = node_type as u8;
}


/**
 * fn leaf_node_num_cells(node: &[u8; PAGE_SIZE]) -> usize {
    let value = u32::from_le_bytes([
        node[LEAF_NODE_NUM_CELLS_OFFSET],
        node[LEAF_NODE_NUM_CELLS_OFFSET + 1],
        node[LEAF_NODE_NUM_CELLS_OFFSET + 2],
        node[LEAF_NODE_NUM_CELLS_OFFSET + 3],
    ]);
    value as usize
}
 */

fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let page_num = cursor.page_num;
    let node = get_page(&mut cursor.table.pager, page_num).expect("Failed to get page");

    let num_cells = leaf_node_num_cells(node);

    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        leaf_node_split_and_insert(cursor, key, value);

        return;
    }

    // Make room for the new cell if inserting in the middle
    if cursor.cell_num < num_cells as usize {
        // Move cells to make room - iterate in reverse to avoid overwriting
        for i in ((cursor.cell_num + 1)..=num_cells as usize).rev() {
            let src_offset = leaf_node_cell_offset(i - 1);
            let dest_offset = leaf_node_cell_offset(i);
            
            // Copy the cell data
            let (left, right) = node.split_at_mut(dest_offset);
            let dest = &mut right[..LEAF_NODE_CELL_SIZE];
            let src = &left[src_offset..src_offset + LEAF_NODE_CELL_SIZE];
            dest.copy_from_slice(src);
        }
    }

    // Update cell count
    set_leaf_node_num_cells(node, num_cells + 1);

    // Insert key at the correct offset
    let key_offset = leaf_node_cell_offset(cursor.cell_num);
    let key_bytes = key.to_le_bytes();
    node[key_offset..key_offset + 4].copy_from_slice(&key_bytes);

    // Serialize value at the correct offset
    let value_offset = leaf_node_cell_offset(cursor.cell_num) + LEAF_NODE_KEY_SIZE;
    let value_dest = &mut node[value_offset..value_offset + ROW_SIZE];
    serialize_row(value, value_dest);
}

fn leaf_node_split_and_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    // Get the old page number first
    let old_page_num = cursor.page_num;
    let new_page_num = get_unused_page_num(&mut cursor.table.pager);
    
    // We need to handle this carefully due to borrowing rules
    // First, get the old next leaf value
    let old_next_leaf = {
        let old_node = get_page(&mut cursor.table.pager, old_page_num)
            .expect("Failed to get old node");
        get_leaf_node_next_leaf(old_node)
    };
    
    // Initialize the new node
    {
        let new_node = get_page(&mut cursor.table.pager, new_page_num)
            .expect("Failed to get new node");
        initialize_leaf_node(new_node);
        set_leaf_node_next_leaf(new_node, old_next_leaf);
    }

    // Get parent page number and set it for the new node
    let parent_page_num = {
        let old_node = get_page(&mut cursor.table.pager, old_page_num)
            .expect("Failed to get old node");
        node_parent(old_node)
    };
    
    {
        let new_node = get_page(&mut cursor.table.pager, new_page_num)
            .expect("Failed to get new node");
        set_node_parent(new_node, parent_page_num);
    }
    
    // Update the old node's next pointer
    {
        let old_node = get_page(&mut cursor.table.pager, old_page_num)
            .expect("Failed to get old node");
        set_leaf_node_next_leaf(old_node, new_page_num as u32);
    }

    // Create temporary storage for all cells (existing + new one)
    let mut all_cells = Vec::with_capacity(LEAF_NODE_MAX_CELLS + 1);
    
    // Collect all existing cells
    {
        let old_node = get_page(&mut cursor.table.pager, old_page_num)
            .expect("Failed to get old node");
        
        for i in 0..LEAF_NODE_MAX_CELLS {
            if i == cursor.cell_num {
                // Insert the new cell at the correct position
                let mut new_cell = vec![0u8; LEAF_NODE_CELL_SIZE];
                // Set key
                new_cell[0..4].copy_from_slice(&key.to_le_bytes());
                // Set value
                serialize_row(value, &mut new_cell[LEAF_NODE_KEY_SIZE..]);
                all_cells.push(new_cell);
                
                // If there are more cells, add the current one
                if i < leaf_node_num_cells(old_node) as usize {
                    let cell_data = leaf_node_cell(old_node, i).to_vec();
                    all_cells.push(cell_data);
                }
            } else if i < leaf_node_num_cells(old_node) as usize {
                let adjust_i = if i > cursor.cell_num { i } else { i };
                let cell_data = leaf_node_cell(old_node, adjust_i).to_vec();
                all_cells.push(cell_data);
            }
        }
        
        // If we're inserting at the end
        if cursor.cell_num >= leaf_node_num_cells(old_node) as usize {
            let mut new_cell = vec![0u8; LEAF_NODE_CELL_SIZE];
            new_cell[0..4].copy_from_slice(&key.to_le_bytes());
            serialize_row(value, &mut new_cell[LEAF_NODE_KEY_SIZE..]);
            all_cells.push(new_cell);
        }
    }

    // Now distribute the cells
    {
        let old_node = get_page(&mut cursor.table.pager, old_page_num)
            .expect("Failed to get old node");
        
        // Copy left split to old node
        for i in 0..LEAF_NODE_LEFT_SPLIT_COUNT {
            if i < all_cells.len() {
                let dest = leaf_node_cell(old_node, i);
                dest.copy_from_slice(&all_cells[i]);
            }
        }
        set_leaf_node_num_cells(old_node, LEAF_NODE_LEFT_SPLIT_COUNT as u32);
    }
    
    {
        let new_node = get_page(&mut cursor.table.pager, new_page_num)
            .expect("Failed to get new node");
        
        // Copy right split to new node
        for i in 0..LEAF_NODE_RIGHT_SPLIT_COUNT {
            let source_index = LEAF_NODE_LEFT_SPLIT_COUNT + i;
            if source_index < all_cells.len() {
                let dest = leaf_node_cell(new_node, i);
                dest.copy_from_slice(&all_cells[source_index]);
            }
        }
        set_leaf_node_num_cells(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT as u32);
    }

    // Check if we need to create a new root
    let is_root = {
        let old_node = get_page(&mut cursor.table.pager, old_page_num)
            .expect("Failed to get old node");
        is_node_root(old_node)
    };
    
    if is_root {
        create_new_root(&mut cursor.table, new_page_num);
    } else {
        // 1. Get max key of old_node after split
        let old_max = get_node_max_key(&mut cursor.table.pager, old_page_num);

        // 2. Get the parent page number
        let parent_page_num = {
            let old_node = get_page(&mut cursor.table.pager, old_page_num)
                .expect("Failed to get old node after split");
            node_parent(old_node) as usize
        };

        // 3. Assign the same parent to the new node (already done above)

        // 4. Get max key of old_node again (it may have changed)
        let new_max = get_node_max_key(&mut cursor.table.pager, old_page_num);

        // 5. Load the parent page and update the key
        {
            let parent = get_page(&mut cursor.table.pager, parent_page_num)
                .expect("Failed to load parent page");
            update_internal_node_key(parent, old_max, new_max);
        }

        // 6. Insert the new_node into the parent
        internal_node_insert(&mut cursor.table, parent_page_num, new_page_num);
    }
}

fn node_parent(node: &[u8]) -> u32 {
    let offset = PARENT_POINTER_OFFSET as usize;
    let bytes = &node[offset..offset + std::mem::size_of::<u32>()];
    u32::from_le_bytes(bytes.try_into().expect("Failed to read parent pointer"))
}

//setter function
fn set_node_parent(node: &mut [u8], parent_page_num: u32) {
    let offset = PARENT_POINTER_OFFSET as usize;
    node[offset..offset + 4].copy_from_slice(&parent_page_num.to_le_bytes());
}

fn internal_node_split_and_insert(table: &mut Table, parent_page_num: usize, child_page_num: usize) {
    let old_page_num = parent_page_num;
    
    // Get the old node's max key before any modifications
    let old_max = get_node_max_key(&mut table.pager, parent_page_num);

    // Get the child's max key
    let child_max = get_node_max_key(&mut table.pager, child_page_num);

    let new_page_num = get_unused_page_num(&mut table.pager);

    // Check if we're splitting the root
    let splitting_root = {
        let old_node = get_page(&mut table.pager, old_page_num)
            .expect("Failed to get old node");
        is_node_root(old_node)
    };

    let (actual_old_page_num, parent_page_num) = if splitting_root {
        // Create new root and get the new structure
        create_new_root(table, new_page_num);
        
        // Get the new left child page number (which is where old content moved)
        let parent = get_page(&mut table.pager, table.root_page_num)
            .expect("Failed to get new root");
        let left_child_page_num = *internal_node_child(parent, 0) as usize;
        
        (left_child_page_num, table.root_page_num)
    } else {
        // Initialize the new node
        {
            let new_node = get_page(&mut table.pager, new_page_num)
                .expect("Failed to get new node");
            initialize_internal_node(new_node);
        }
        
        // Get parent page number
        let parent_page_num = {
            let old_node = get_page(&mut table.pager, old_page_num)
                .expect("Failed to get old node");
            node_parent(old_node) as usize
        };
        
        (old_page_num, parent_page_num)
    };

    // Get the right child of the old node before we start moving things
    let cur_page_num = {
        let old_node = get_page(&mut table.pager, actual_old_page_num)
            .expect("Failed to get old node");
        *internal_node_right_child(old_node)
    };

    // First, put the right child into the new node and invalidate old node's right child
    internal_node_insert(table, new_page_num, cur_page_num as usize);
    
    // Update the moved child's parent pointer
    {
        let cur_child = get_page(&mut table.pager, cur_page_num as usize)
            .expect("Failed to get current child");
        set_node_parent(cur_child, new_page_num as u32);
    }
    
    // Set old node's right child to invalid
    {
        let old_node = get_page(&mut table.pager, actual_old_page_num)
            .expect("Failed to get old node");
        set_internal_node_right_child(old_node, INVALID_PAGE_NUM);
    }

    // Move keys and children from old node to new node
    // We need to be careful with borrowing here
    let mut keys_to_move = Vec::new();
    let old_num_keys = {
        let old_node = get_page(&mut table.pager, actual_old_page_num)
            .expect("Failed to get old node");
        let num_keys = *internal_node_num_keys(old_node);
        
        // Collect the keys and children we need to move (from right to left)
        for i in ((INTERNAL_NODE_MAX_CELLS / 2 + 1)..INTERNAL_NODE_MAX_CELLS).rev() {
            if i < num_keys as usize {
                let child_page_num = *internal_node_child(old_node, i);
                keys_to_move.push((i, child_page_num));
            }
        }
        num_keys
    };

    // Now move the collected keys and children
    for (_i, child_page_num) in keys_to_move {
        internal_node_insert(table, new_page_num, child_page_num as usize);
        
        // Update the child's parent pointer
        {
            let child = get_page(&mut table.pager, child_page_num as usize)
                .expect("Failed to get child");
            set_node_parent(child, new_page_num as u32);
        }
        
        // Decrement the old node's key count
        {
            let old_node = get_page(&mut table.pager, actual_old_page_num)
                .expect("Failed to get old node");
            let current_keys = *internal_node_num_keys(old_node);
            *internal_node_num_keys(old_node) = current_keys - 1;
        }
    }

    // Set the child before the middle key to be the old node's right child
    {
        let old_node = get_page(&mut table.pager, actual_old_page_num)
            .expect("Failed to get old node");
        let num_keys = *internal_node_num_keys(old_node);
        let right_child_page_num = *internal_node_child(old_node, num_keys as usize - 1);
        
        set_internal_node_right_child(old_node, right_child_page_num);
        *internal_node_num_keys(old_node) = num_keys - 1;
    }

    // Determine which node should contain the child to be inserted
    let max_after_split = get_node_max_key(&mut table.pager, actual_old_page_num);

    let destination_page_num = if child_max < max_after_split {
        actual_old_page_num
    } else {
        new_page_num
    };

    // Insert the child into the appropriate node
    internal_node_insert(table, destination_page_num, child_page_num);
    
    // Update the child's parent pointer
    {
        let child = get_page(&mut table.pager, child_page_num)
            .expect("Failed to get child");
        set_node_parent(child, destination_page_num as u32);
    }

    // Update the parent's key that pointed to the old node
    {
        let new_old_max = get_node_max_key(&mut table.pager, actual_old_page_num);
        let parent = get_page(&mut table.pager, parent_page_num)
            .expect("Failed to get parent");
        update_internal_node_key(parent, old_max, new_old_max);
    }

    // If we're not splitting the root, insert the new node into its parent
    if !splitting_root {
        let parent_of_old = {
            let old_node = get_page(&mut table.pager, actual_old_page_num)
                .expect("Failed to get old node");
            node_parent(old_node)
        };
        
        internal_node_insert(table, parent_of_old as usize, new_page_num);
        
        // Set the new node's parent
        {
            let new_node = get_page(&mut table.pager, new_page_num)
                .expect("Failed to get new node");
            set_node_parent(new_node, parent_of_old);
        }
    }
}

//creating a new root
fn create_new_root(table: &mut Table, right_child_page_num: usize) {
    let root_page_num = table.root_page_num;
    let left_child_page_num = get_unused_page_num(&mut table.pager);

    // First, get data we need from the root
    let (root_is_internal, root_data) = {
        let root = get_page(&mut table.pager, root_page_num).expect("Failed to get root");
        let is_internal = get_node_type(root) == NodeType::Internal;
        let data = root.to_vec(); // Copy the data
        (is_internal, data)
    };

    // Initialize the children based on root type
    if root_is_internal {
        let right_child = get_page(&mut table.pager, right_child_page_num).expect("Failed to get right child");
        initialize_internal_node(right_child);
        
        let left_child = get_page(&mut table.pager, left_child_page_num).expect("Failed to get left child");
        initialize_internal_node(left_child);
    }

    // Copy the old root's data into the new left child
    {
        let left_child = get_page(&mut table.pager, left_child_page_num).expect("Failed to get left child");
        left_child.copy_from_slice(&root_data);
        set_node_root(left_child, false);
    }

    // If left child is internal, update its children's parent pointers
    if root_is_internal {
        let (num_keys, right_page_num) = {
            let left_child = get_page(&mut table.pager, left_child_page_num).expect("Failed to get left child");
            let num_keys = *internal_node_num_keys(left_child);
            let right_page_num = *internal_node_right_child(left_child);
            (num_keys, right_page_num)
        };

        // Update children's parent pointers
        for i in 0..num_keys {
            let child_page_num = {
                let left_child = get_page(&mut table.pager, left_child_page_num).expect("Failed to get left child");
                *internal_node_child(left_child, i as usize)
            };
            
            let child = get_page(&mut table.pager, child_page_num as usize)
                .expect("Failed to get internal child");
            set_node_parent(child, left_child_page_num as u32);
        }

        // Also update the right child of the internal node
        if right_page_num != INVALID_PAGE_NUM {
            let right = get_page(&mut table.pager, right_page_num as usize)
                .expect("Failed to get internal right child");
            set_node_parent(right, left_child_page_num as u32);
        }
    }

    // Get the left max key before reinitializing root
    let left_max_key = get_node_max_key(&mut table.pager, left_child_page_num);

    // Re-initialize the root as a fresh internal node with two children
    {
        let root = get_page(&mut table.pager, root_page_num).expect("Failed to get root");
        initialize_internal_node(root);
        set_node_root(root, true);
        *internal_node_num_keys(root) = 1;
        *internal_node_child(root, 0) = left_child_page_num as u32;
        *internal_node_key(root, 0) = left_max_key;
        *internal_node_right_child(root) = right_child_page_num as u32;
    }

    // Set parent pointers for new children
    {
        let left_child = get_page(&mut table.pager, left_child_page_num).expect("Failed to get left child");
        set_node_parent(left_child, root_page_num as u32);
    }
    
    {
        let right_child = get_page(&mut table.pager, right_child_page_num).expect("Failed to get right child");
        set_node_parent(right_child, root_page_num as u32);
    }
}

//To do this in Rust
fn get_unused_page_num(pager: &mut Pager) -> usize {
	pager.num_pages
}


struct Table {
    root_page_num: usize,
    pager: Box<Pager>, // Changed from 'pages' to 'pager'
}

impl Table {
    pub fn new() -> Self {
        // Create a temporary file or use a default file path
        let file = File::create("database.db").expect("Failed to create database file");
        
        let pager = Pager {
            file_descriptor: file,
            file_length: 0,
            pages: std::array::from_fn(|_| None),
            num_pages: 0, // Initialize num_pages to 0
        };
        
        Self {
            pager: Box::new(pager),
            root_page_num: 0, // Changed from 'pages' to 'pager'
        }
    }

}

pub fn cursor_value<'a>(cursor: &'a mut Cursor) -> Option<&'a [u8]> {
    let page_num = cursor.page_num;
    let cell_num = cursor.cell_num;

    let page = get_page(&mut cursor.table.pager, page_num)?;
    Some(leaf_node_value(page, cell_num))
}

fn cursor_advance(cursor: &mut Cursor) {
    let page_num = cursor.page_num;

    // Load the node safely
    let node = match get_page(&mut cursor.table.pager, page_num) {
        Some(node) => node,
        None => {
            eprintln!("Failed to load page {}", page_num);
            cursor.end_of_table = true;
            return;
        }
    };

    cursor.cell_num += 1;

    // If we've exhausted the cells in this leaf
    let num_cells = leaf_node_num_cells(node);
    if cursor.cell_num >= num_cells as usize {
        let next_page_num = get_leaf_node_next_leaf(node);

        if next_page_num == 0 {
            // This is the rightmost leaf node
            cursor.end_of_table = true;
        } else {
            // Jump to the next leaf node
            cursor.page_num = next_page_num as usize;
            cursor.cell_num = 0;
        }
    }
}

fn get_page(pager: &mut Pager, page_num: usize) -> Option<&mut [u8; PAGE_SIZE]> {
    if page_num >= TABLE_MAX_PAGES {
        println!(
            "Tried to fetch page number out of bounds. {} > {}",
            page_num, TABLE_MAX_PAGES
        );
        process::exit(1);
    }

    if pager.pages[page_num].is_none() {
        // Cache miss
        let mut page = Box::new([0u8; PAGE_SIZE]);
        let num_pages = (pager.file_length / PAGE_SIZE as u64) as usize;
        let has_partial_page = pager.file_length % PAGE_SIZE as u64 != 0;

        if page_num < num_pages || (page_num == num_pages && has_partial_page) {
            // Seek to the correct position
            if let Err(e) = pager
                .file_descriptor
                .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            {
                println!("Seek error: {}", e);
                process::exit(1);
            }

            // Calculate how many bytes to read
            let bytes_to_read = if page_num < num_pages {
                PAGE_SIZE
            } else {
                // This is a partial page
                (pager.file_length % PAGE_SIZE as u64) as usize
            };

            // Read only the bytes that exist in the file
            if let Err(e) = pager.file_descriptor.read_exact(&mut page[..bytes_to_read]) {
                println!("Read error: {}", e);
                process::exit(1);
            }
        }

        pager.pages[page_num] = Some(page);
        if page_num >= pager.num_pages{
	        pager.num_pages = page_num + 1;
        }
    }

    pager.pages[page_num].as_deref_mut()
}

fn db_open(filename: &str) -> std::io::Result<Table> {
    let mut pager = pager_open(filename)?;
    let root_page_num = 0;

    if pager.num_pages == 0 {
        // New DB file — initialize page 0 as a leaf node.
        if let Some(root_node) = get_page(&mut pager, root_page_num) {
            initialize_leaf_node(root_node);
            set_node_root(root_node, true);
        } else {
            eprintln!("Failed to initialize root page");
        }
    }

    Ok(Table {
        pager: Box::new(pager),
        root_page_num,
    })
}


fn pager_open(filename: &str) -> io::Result<Pager> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(filename) {
        Ok(file) => file,
        Err(_) => {
            println!("Unable to open file");
            process::exit(1);
        }
    };
    
    let file_length = file.seek(SeekFrom::End(0))?;
    if file_length % PAGE_SIZE as u64 != 0 {
        eprintln!("Db file is not a whole number of pages. Corrupt file.");
        process::exit(1);
    }

    let num_pages = (file_length / PAGE_SIZE as u64) as usize;
    let pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES] = 
        std::array::from_fn(|_| None);
    
    
    Ok(Pager {
        file_descriptor: file,
        file_length,
        num_pages,
        pages,
    })
}

fn db_close(table: &mut Table) {
    let pager = &mut table.pager;


    for i in 0..pager.num_pages {
        if let Some(_) = pager.pages[i] {
            pager_flush(pager, i);
            pager.pages[i] = None; // Drop the page
        }
    }

    // Flush and close the file
    if let Err(e) = pager.file_descriptor.sync_all() {
        eprintln!("Error syncing db file: {}", e);
        process::exit(1);
    }

    // Drop any remaining in-memory pages
    for page_slot in pager.pages.iter_mut() {
        if page_slot.is_some() {
            *page_slot = None;
        }
    }

    println!("Database closed cleanly.");
}

fn pager_flush(pager: &mut Pager, page_num: usize) {
    if pager.pages[page_num].is_none() {
        eprintln!("Tried to flush None page");
        process::exit(1);
    }

    // Seek to the correct position
    let offset = match pager.file_descriptor.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)) {
        Ok(offset) => offset,
        Err(e) => {
            eprintln!("Error seeking: {}", e);
            process::exit(1);
        }
    };

    // Write the page data
    let page_data = pager.pages[page_num].as_ref().unwrap();
    let bytes_to_write = &page_data[..PAGE_SIZE];

    if let Err(e) = pager.file_descriptor.write_all(bytes_to_write) {
        eprintln!("Error writing: {}", e);
        process::exit(1);
    }
}


#[derive(Debug)]
enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
}

enum PrepareResult {
    Success(Statement),
    StringTooLong,
    SyntaxError,
    NegativeId,
    UnrecognizedStatement,
}

enum ExecuteResult {
    Success,
    TableFull,
    DuplicateKey,
}

#[derive(Debug)]
enum StatementType {
    Select,
    Insert,
}
#[repr(C)]
#[derive(Debug)]
pub struct Row {
    pub id: u32,
    pub username: [u8; COLUMN_USERNAME_SIZE],
    pub email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    pub fn serialize_row(&self, destination: &mut [u8]) {
        assert!(destination.len() >= ROW_SIZE, "Destination buffer too small");

        //serialze the Id
        destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&self.id.to_le_bytes());

        //serilaize the username
        destination[USERNAME_OFFSET..USERNAME_OFFSET + COLUMN_USERNAME_SIZE].copy_from_slice(&self.username);

        //serialize the email
        destination[EMAIL_OFFSET..EMAIL_OFFSET + COLUMN_EMAIL_SIZE].copy_from_slice(&self.email);
    }

    pub fn deserialize(source: &[u8]) -> Self {
        assert!(source.len() >= ROW_SIZE, "Source buffer too small");

        let mut id_bytes = [0u8; 4];
        id_bytes.copy_from_slice(&source[ID_OFFSET..ID_OFFSET + ID_SIZE]);
        let id = u32::from_le_bytes(id_bytes);

        let mut username = [0u8; USERNAME_SIZE];
        username.copy_from_slice(&source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]);

        let mut email = [0u8; EMAIL_SIZE];
        email.copy_from_slice(&source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]);

        Self { id, username, email }
    }

    // Helper method to get username as string
    pub fn get_username(&self) -> String {
        // Find the first null byte or use the entire array
        let end = self.username.iter().position(|&x| x == 0).unwrap_or(self.username.len());
        String::from_utf8_lossy(&self.username[..end]).to_string()
    }
    
    // Helper method to get email as string
    pub fn get_email(&self) -> String {
        // Find the first null byte or use the entire array
        let end = self.email.iter().position(|&x| x == 0).unwrap_or(self.email.len());
        String::from_utf8_lossy(&self.email[..end]).to_string()
    }
}

fn serialize_row(row: &Row, destination: &mut [u8]) {
    row.serialize_row(destination);
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

// Helper function to indent output based on depth
fn indent(level: usize) {
    for _ in 0..level {
        print!("  "); // Two spaces per indent level
    }
}

// Recursive function to print the B-tree starting from any page
fn print_tree(pager: &mut Pager, page_num: usize, indentation_level: usize) {
    // First, collect all the data we need from the node
    let (node_type, num_keys, keys, children, right_child) = {
        let node = get_page(pager, page_num).expect("Failed to get page");
        let node_type = get_node_type(node);
        
        match node_type {
            NodeType::Leaf => {
                let num_keys = leaf_node_num_cells(node);
                let mut keys = Vec::new();
                for i in 0..num_keys {
                    keys.push(leaf_node_key(node, i as usize));
                }
                (node_type, num_keys, keys, Vec::new(), 0)
            }
            NodeType::Internal => {
                let num_keys = *internal_node_num_keys(node);
                let mut children = Vec::new();
                let mut keys = Vec::new();
                
                for i in 0..num_keys {
                    children.push(*internal_node_child(node, i as usize));
                    keys.push(*internal_node_key(node, i as usize));
                }
                let right_child = *internal_node_right_child(node);
                
                (node_type, num_keys, keys, children, right_child)
            }
        }
    };

    // Now process the data without holding any borrows
    match node_type {
        NodeType::Leaf => {
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);

            for key in keys {
                indent(indentation_level + 1);
                println!("- {}", key);
            }
        }

        NodeType::Internal => {
            indent(indentation_level);
            println!("- internal (size {})", num_keys);

            // Process children and keys
            for i in 0..num_keys as usize {
                let child = children[i];
                
                if child == INVALID_PAGE_NUM {
                    indent(indentation_level + 1);
                    println!("- <empty child>");
                    continue;
                }

                print_tree(pager, child as usize, indentation_level + 1);
                indent(indentation_level + 1);
                println!("- key {}", keys[i]);
            }

            // Handle right child
            if right_child != INVALID_PAGE_NUM {
                print_tree(pager, right_child as usize, indentation_level + 1);
            } else {
                indent(indentation_level + 1);
                println!("- <empty right child>");
            }
        }
    }
}


fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}


fn do_meta_command(input: &InputBuffer, table: &mut Table) -> MetaCommandResult {
    match input.buffer.trim() {
        ".exit" => {
            db_close(table);
            std::process::exit(0);
        }
        ".btree" => {
            println!("Tree:");
            print_tree(&mut table.pager, 0, 0);
            MetaCommandResult::Success
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
            MetaCommandResult::Success
        }
        _ => MetaCommandResult::UnrecognizedCommand,
    }
}

fn prepare_statement(input_buffer: &InputBuffer) -> PrepareResult {
    let input = input_buffer.buffer.trim();

    if input.starts_with("insert") {
        // Parse as i32 first to catch negative numbers
        let parsed = scan_fmt!(input, "insert {} {} {}", i32, String, String);

        match parsed {
            Ok((id, username, email)) => {
                // Check if id is negative FIRST
                if id < 0 {
                    return PrepareResult::NegativeId;
                }
                
                // Convert to u32 now that we know it's positive
                let id = id as u32;
                
                // Convert strings to fixed-size byte arrays
                let mut username_bytes = [0u8; COLUMN_USERNAME_SIZE];
                let mut email_bytes = [0u8; COLUMN_EMAIL_SIZE];
                
                // Check if username is too long
                if username.len() > COLUMN_USERNAME_SIZE {
                    return PrepareResult::StringTooLong;
                }
                
                // Check if email is too long
                if email.len() > COLUMN_EMAIL_SIZE {
                    return PrepareResult::StringTooLong;
                }
                
                // Copy the string bytes into the arrays
                username_bytes[..username.len()].copy_from_slice(username.as_bytes());
                email_bytes[..email.len()].copy_from_slice(email.as_bytes());
                
                let row = Row { 
                    id, 
                    username: username_bytes, 
                    email: email_bytes 
                };
                
                let statement = Statement {
                    statement_type: StatementType::Insert,
                    row_to_insert: Some(row),
                };
                return PrepareResult::Success(statement);
            }
            Err(_) => return PrepareResult::SyntaxError,
        }
    }

    if input == "select" {
        let statement = Statement {
            statement_type: StatementType::Select,
            row_to_insert: None,
        };
        return PrepareResult::Success(statement);
    }

    PrepareResult::UnrecognizedStatement
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    let row_to_insert = match &statement.row_to_insert {
        Some(row) => row,
        None => return ExecuteResult::TableFull,
    };

    let key_to_insert = row_to_insert.id;
    let mut cursor = table_find(table, key_to_insert as usize);

    // Get page again to check for duplicate keys
    let page_num = cursor.page_num;
    let node = match get_page(&mut cursor.table.pager, page_num) {
        Some(n) => n,
        None => return ExecuteResult::TableFull,
    };

    let num_cells = leaf_node_num_cells(node);

   /*if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        return ExecuteResult::TableFull;
    }
    */

    if cursor.cell_num < num_cells as usize {
        let key_at_index = leaf_node_key(node, cursor.cell_num);
        if key_at_index == key_to_insert {
            return ExecuteResult::DuplicateKey;
        }
    }

    leaf_node_insert(&mut cursor, row_to_insert.id, row_to_insert);

    ExecuteResult::Success
}


fn execute_select(_statement: &Statement, table: &mut Table) -> ExecuteResult {
    let mut cursor = table_start(table);

    while !cursor.end_of_table {
        if let Some(slot) = cursor_value(&mut cursor) {
            let row = Row::deserialize(slot);
            println!("({}, {}, {})", row.id, row.get_username(), row.get_email());
        } else {
            break;
        }
        cursor_advance(&mut cursor);
    }

    ExecuteResult::Success
}

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match statement.statement_type {
        StatementType::Insert => execute_insert(statement, table),
        StatementType::Select => execute_select(statement, table),
    }
}

struct InputBuffer {
    buffer: String,
    buffer_length: usize,
    input_length: usize,
}

impl InputBuffer {
    fn new() -> Self {
        Self {
            buffer: String::new(),
            buffer_length: 0,
            input_length: 0,
        }
    }

    fn read_input(&mut self) {
        self.buffer.clear();
        print!("db > ");
        io::stdout().flush().unwrap();

        if let Err(error) = io::stdin().read_line(&mut self.buffer) {
            eprintln!("Error reading input: {}", error);
            std::process::exit(1);
        }

        let trimmed = self.buffer.trim_end();
        self.input_length = trimmed.len();
        self.buffer_length = self.buffer.capacity();
        self.buffer = trimmed.to_string();
    }
}

fn main() {
    // Get the command line arguments
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Must supply a database filename.");
        process::exit(1);
    }
    // Open the database file
    let filename = &args[1];
    let mut table = db_open(filename).expect("Failed to open database");
    
    
    let mut input_buffer = InputBuffer::new();

    println!("ID_SIZE: {}", ID_SIZE);
    println!("USERNAME_SIZE: {}", USERNAME_SIZE);
    println!("EMAIL_SIZE: {}", EMAIL_SIZE);
    println!("ID_OFFSET: {}", ID_OFFSET);
    println!("USERNAME_OFFSET: {}", USERNAME_OFFSET);
    println!("EMAIL_OFFSET: {}", EMAIL_OFFSET);
    println!("ROW_SIZE: {}", ROW_SIZE);

    loop {
        input_buffer.read_input();
        
        if input_buffer.buffer.starts_with('.') {
            match do_meta_command(&input_buffer, &mut table) {
                MetaCommandResult::Success => continue,
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command '{}'.", input_buffer.buffer);
                    continue;
                }
            }
        }

        match prepare_statement(&input_buffer) {
            PrepareResult::Success(statement) => {
                let result = execute_statement(&statement, &mut table);
                match result {
                    ExecuteResult::Success => {
                        println!("Executed successfully.");
                    }
                    ExecuteResult::DuplicateKey => {
                        println!("Error: Duplicate key.");
                    }
                    ExecuteResult::TableFull => {
                        println!("Error: Table full.");
                    }
                }
            }
            PrepareResult::NegativeId => {
                println!("Error: ID must be positive.");
                continue;
            }
            PrepareResult::StringTooLong => {
                println!("Error: String too long.");
                continue;   
            }
            PrepareResult::SyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            PrepareResult::UnrecognizedStatement => {
                println!(
                    "Unrecognized keyword at start of '{}'",
                    input_buffer.buffer
                );
                continue;
            }
        }
    }
}


