#!/usr/bin/env python3
"""
BTEQ Complete Chunking Pipeline

This script performs TWO-STAGE chunking on large BTEQ scripts:

STAGE 1: Label-based chunking
  - Splits script at .label boundaries
  - Analyzes .goto references (conditional vs unconditional)
  - Inserts .goto for consecutive labels to preserve fall-through
  - Preserves conditional labels as .label for if-else logic
  - Converts unconditional-only labels to --FUNCTION:
  - Detects section boundaries to prevent incorrect .goto insertion
  - Uses smart continuation label detection for CREATE→COLLECT sequences
  - Removes empty initialization chunks (only label comments)

STAGE 2: Sub-chunking (automatic for blocks >1200 lines)
  - Further divides large blocks into manageable chunks
  - Splits at temp table, result table, nested job boundaries
  - Preserves 100% code continuity with chronological order

CROSS-CHUNK GOTO: Smart .goto insertion between final chunks
  - If next chunk starts with .label and current chunk has no unconditional .goto (including --CONTINUE:), add .goto

Usage:
    python "BTeqLabelParser - SubChunker.py" input_script.sql [--threshold 1200]

Input: Single large BTEQ script (e.g., 23,703 lines)
Output: Single folder with optimally-sized chunks (<1200 lines each)
"""

import re
from pathlib import Path
from typing import List, Tuple, Set
import argparse
from datetime import datetime


class CodeBlock:
    """Represents any type of BTEQ code block"""
    def __init__(self, start_line: int, end_line: int, block_type: str, name: str, lines: List[str]):
        self.start_line = start_line
        self.end_line = end_line
        self.block_type = block_type
        self.name = name
        self.lines = lines
        self.line_count = len(lines)
    
    def __repr__(self):
        return f"CodeBlock({self.block_type}, {self.name}, {self.line_count} lines)"


class BteqCompletePipeline:
    def __init__(self, input_file: str, line_threshold: int = 1200):
        self.input_file = Path(input_file)
        self.line_threshold = line_threshold
        self.output_folder = self.input_file.parent / f"{self.input_file.stem}_chunks"
        
        self.content = ""
        self.processed_content = ""
        self.conditional_goto_refs = set()
        self.all_goto_refs = set()
        
        # Block markers for stage 2
        self.block_markers = {
            'TEMP_TABLE_START': r'/\*--Temporary table-+\*/',
            'TEMP_TABLE_END': r'/\*--End Temporary table-+\*/',
            'RESULT_TABLE_START': r'/\*--Result table-+\*/',
            'RESULT_TABLE_END': r'/\*--End Result table-+\*/',
            'RESULTVIEW_START': r'/\*[!]*RESULTVIEW-+\*/',
            'RESULTVIEW_END': r'/\*--END RESULTVIEW-+\*/',
            'RESULTABLE_START': r'/\*[!]*RESULTABLE-+\*/',
            'RESULTABLE_END': r'/\*--END RESULTABLE-+\*/',
            'SECTION_START': r'/\*--BEGIN Shared temporary table.*?\*/',
            'SECTION_END': r'/\*--END Shared temporary table.*?\*/',
            'NESTED_JOB_START': r'/\*--Nested Job-+\*/',
            'NESTED_JOB_END': r'/\*--End Nested Job-+\*/',
        }
    
    # ============================================================================
    # STAGE 1: Label-based chunking
    # ============================================================================
    
    def read_file(self):
        """Read the input BTEQ file"""
        with open(self.input_file, 'r', encoding='utf-8') as f:
            self.content = f.read()
        print(f"✓ Read file: {self.input_file.name} ({len(self.content):,} characters)")
    
    def find_all_goto_references(self) -> Set[str]:
        """Find ALL .goto statements and extract referenced label names"""
        all_goto_refs = set()
        lines = self.content.split('\n')
        
        for line in lines:
            goto_match = re.search(r'\.goto\s+(\w+)', line, re.IGNORECASE)
            if goto_match:
                label_name = goto_match.group(1).upper()
                all_goto_refs.add(label_name)
        
        return all_goto_refs
    
    def find_conditional_goto_references(self) -> Set[str]:
        """Find .goto statements inside .if conditions"""
        conditional_goto_refs = set()
        lines = self.content.split('\n')
        
        for i, line in enumerate(lines):
            # Single-line conditional: .IF condition THEN .GOTO LABELNAME
            if_goto_match = re.search(r'\.IF\s+.*\.GOTO\s+(\w+)', line, re.IGNORECASE)
            if if_goto_match:
                label_name = if_goto_match.group(1).upper()
                conditional_goto_refs.add(label_name)
        
        return conditional_goto_refs
    
    def find_label_positions(self, lines: List[str]) -> List[Tuple[int, str]]:
        """Find all label positions in the content"""
        label_positions = []
        
        for i, line in enumerate(lines):
            label_match = re.search(r'^\s*\.label\s+(\w+)', line, re.IGNORECASE)
            if label_match:
                label_name = label_match.group(1).upper()
                label_positions.append((i, label_name))
        
        return label_positions
    
    def has_goto_between(self, lines: List[str], start_idx: int, end_idx: int) -> bool:
        """Check if there's any .goto statement between two line indices"""
        for i in range(start_idx, end_idx):
            if i < len(lines) and re.search(r'^\s*\.goto\s+', lines[i], re.IGNORECASE):
                return True
        return False
    
    def has_section_boundary(self, lines: List[str], start_idx: int, end_idx: int) -> bool:
        """Check if there's a section boundary marker between two indices
        
        Section boundaries are marked by: /*--.label---*/
        These indicate independent sections that should NOT have .goto inserted between them
        """
        for i in range(start_idx, end_idx):
            if i < len(lines) and re.search(r'/\*--\.label-+\*/', lines[i]):
                return True
        return False
    
    def is_continuation_label(self, lines: List[str], current_pos: int, next_pos: int, next_name: str) -> bool:
        """
        Detect if next_name is a continuation label that should NOT get fall-through .goto
        Key insight: If a label is already referenced by ANY goto, it's a destination label
        """
        # If the label is already referenced by ANY goto, it's a destination label
        # and should NOT get additional fall-through .goto
        if next_name in self.all_goto_refs:
            return True
        
        # Check if next label starts with continuation operations
        if next_pos + 1 < len(lines):
            next_content = ""
            for i in range(next_pos + 1, min(next_pos + 10, len(lines))):
                if i < len(lines):
                    next_content += lines[i].strip().upper() + " "
            
            # Continuation operations that shouldn't get fall-through .goto
            continuation_ops = [
                "COLLECT STATISTICS",
                "GRANT SELECT", 
                "CREATE INDEX",
                "ALTER TABLE",
                "INSERT INTO",
                "UPDATE",
                "DELETE FROM"
            ]
            
            for op in continuation_ops:
                if op in next_content:
                    return True
        
        return False
    
    def process_labels(self):
        """Convert labels based on goto analysis and insert gotos for fall-through"""
        lines = self.content.split('\n')
        processed_lines = []
        
        self.all_goto_refs = self.find_all_goto_references()
        self.conditional_goto_refs = self.find_conditional_goto_references()
        
        print(f"\n📊 GOTO Analysis:")
        print(f"   Total goto references: {len(self.all_goto_refs)}")
        print(f"   Conditional gotos: {len(self.conditional_goto_refs)}")
        print(f"   All referenced labels: {sorted(self.all_goto_refs)}")
        
        # Find all label positions
        label_positions = self.find_label_positions(lines)
        
        unconditional_only = self.all_goto_refs - self.conditional_goto_refs
        
        # First pass: Convert labels and gotos
        for i, line in enumerate(lines):
            label_match = re.search(r'^\s*\.label\s+(\w+)', line, re.IGNORECASE)
            
            if label_match:
                label_name = label_match.group(1).upper()
                original_indent = re.match(r'^(\s*)', line).group(1)
                
                # If label is referenced by conditional goto, keep as .label
                if label_name in self.conditional_goto_refs:
                    processed_lines.append(line)  # Keep active .label
                # If only referenced by unconditional gotos, convert to --FUNCTION:
                elif label_name in unconditional_only:
                    processed_lines.append(f"{original_indent}--FUNCTION: {label_name}")
                # Not referenced at all
                else:
                    processed_lines.append(f"{original_indent}--SECTION: {label_name}")
            
            elif re.search(r'^\s*\.goto\s+(\w+)', line, re.IGNORECASE):
                # Check if this is a conditional goto (part of .if statement)
                if re.search(r'\.if\s+', line, re.IGNORECASE):
                    processed_lines.append(line)  # Keep conditional goto as-is
                else:
                    # Unconditional goto
                    goto_match = re.search(r'^\s*\.goto\s+(\w+)', line, re.IGNORECASE)
                    label_name = goto_match.group(1).upper()
                    original_indent = re.match(r'^(\s*)', line).group(1)
                    
                    # If target label is referenced by conditional goto, keep as .goto
                    if label_name in self.conditional_goto_refs:
                        processed_lines.append(line)  # Keep as .goto (for function call)
                    else:
                        # Target only referenced unconditionally, convert to comment
                        processed_lines.append(f"{original_indent}--CONTINUE: {label_name}")
            else:
                processed_lines.append(line)
        
        # Second pass: Insert .goto statements AFTER .If errorcode checks for fall-through
        # BUT ONLY if there's no section boundary marker AND next label is not a continuation
        final_lines = []
        labels_needing_goto = {}  # Map: label_line_index -> next_label_name
        
        # Identify which labels need .goto inserted at their end
        for i in range(len(label_positions) - 1):
            current_pos, current_name = label_positions[i]
            next_pos, next_name = label_positions[i + 1]
            
            # Skip .goto insertion if:
            # 1. Explicit .goto already exists between labels
            # 2. Section boundary marker exists (independent sections)
            # 3. Next label is a continuation pattern (referenced by goto + continuation operation)
            if (not self.has_goto_between(lines, current_pos + 1, next_pos) and
                not self.has_section_boundary(lines, current_pos + 1, next_pos) and
                not self.is_continuation_label(lines, current_pos, next_pos, next_name)):
                labels_needing_goto[current_pos] = next_name
        
        print(f"\n📋 Fall-through .goto insertion analysis:")
        print(f"   Labels needing .goto: {len(labels_needing_goto)}")
        for pos, target in labels_needing_goto.items():
            # Find label name at this position
            label_name = "unknown"
            for lpos, lname in label_positions:
                if lpos == pos:
                    label_name = lname
                    break
            print(f"   - {label_name} → {target}")
        
        # Debug continuation label detection
        print(f"\n🔍 Destination/Continuation label analysis:")
        for i in range(len(label_positions) - 1):
            current_pos, current_name = label_positions[i]
            next_pos, next_name = label_positions[i + 1]
            is_continuation = self.is_continuation_label(lines, current_pos, next_pos, next_name)
            if is_continuation:
                reason = "referenced by goto" if next_name in self.all_goto_refs else "continuation operation"
                print(f"   - {next_name} detected as DESTINATION/CONTINUATION ({reason}) - no .goto will be added")
        
        # Build final output with gotos inserted after .If errorcode checks
        i = 0
        while i < len(processed_lines):
            line = processed_lines[i]
            
            # Check if this is a label that needs goto insertion
            label_match = re.search(r'^\s*(?:\.label|--FUNCTION:|--SECTION:)\s+(\w+)', line, re.IGNORECASE)
            if label_match:
                current_label_name = label_match.group(1).upper()
                
                # Find this label in original positions
                original_line_idx = None
                for pos, name in label_positions:
                    if name == current_label_name:
                        original_line_idx = pos
                        break
                
                if original_line_idx is not None and original_line_idx in labels_needing_goto:
                    # This label needs .goto after last .If errorcode
                    next_label_name = labels_needing_goto[original_line_idx]
                    
                    # Add current label line
                    final_lines.append(line)
                    
                    # Collect lines until we find the last .If errorcode or hit next label
                    j = i + 1
                    last_errorcode_idx = None
                    errorcode_indent = "\t"  # Default indentation
                    temp_lines = []
                    
                    while j < len(processed_lines):
                        next_line = processed_lines[j]
                        
                        # Check if this is .If errorcode line
                        if re.search(r'^\s*\.If\s+errorcode', next_line, re.IGNORECASE):
                            last_errorcode_idx = len(temp_lines)
                            errorcode_indent = re.match(r'^(\s*)', next_line).group(1)
                        
                        # Check if we've reached the next label
                        next_label_check = re.search(r'^\s*(?:\.label|--FUNCTION:|--SECTION:)\s+', next_line, re.IGNORECASE)
                        if next_label_check:
                            # Found next label
                            # Add all collected lines up to last errorcode
                            for k, temp_line in enumerate(temp_lines):
                                final_lines.append(temp_line)
                                if k == last_errorcode_idx:
                                    # Insert blank line + .goto after the .If errorcode line
                                    final_lines.append("")
                                    final_lines.append(f"{errorcode_indent}.goto {next_label_name}")
                            
                            # If no errorcode found, don't insert goto
                            if last_errorcode_idx is None:
                                pass  # Lines already added
                            
                            i = j  # Skip to next label
                            break
                        
                        temp_lines.append(next_line)
                        j += 1
                    else:
                        # Reached end of file
                        for k, temp_line in enumerate(temp_lines):
                            final_lines.append(temp_line)
                            if k == last_errorcode_idx:
                                final_lines.append("")
                                final_lines.append(f"{errorcode_indent}.goto {next_label_name}")
                        i = j
                    
                    continue
            
            final_lines.append(line)
            i += 1
        
        self.processed_content = '\n'.join(final_lines)
    
    def extract_label_blocks(self) -> List[Tuple[str, str, int]]:
        """Extract blocks based on label positions"""
        lines = self.processed_content.split('\n')
        label_positions = []
        
        for i, line in enumerate(lines):
            if (re.search(r'^\s*\.label\s+(\w+)', line, re.IGNORECASE) or
                re.search(r'^\s*--FUNCTION:\s*(\w+)', line, re.IGNORECASE) or
                re.search(r'^\s*--SECTION:\s*(\w+)', line, re.IGNORECASE)):
                
                label_match = re.search(r'^\s*(?:\.label|--FUNCTION:|--SECTION:)\s*(\w+)', line, re.IGNORECASE)
                if label_match:
                    label_name = label_match.group(1).upper()
                    label_positions.append((i, label_name, line))
        
        print(f"\n📑 Found {len(label_positions)} label blocks")
        
        blocks = []
        
        # Initial setup section (before first label)
        if label_positions and label_positions[0][0] > 0:
            initialization_lines = lines[:label_positions[0][0]]
            
            # Check if initialization has meaningful content (not just label comments)
            has_meaningful_content = False
            for line in initialization_lines:
                stripped = line.strip()
                # Skip empty lines and label comment markers
                if stripped and not re.match(r'/\*--\.?label-*\*/', stripped, re.IGNORECASE):
                    has_meaningful_content = True
                    break
            
            if has_meaningful_content:
                blocks.append(("00_initialization", '\n'.join(initialization_lines), len(initialization_lines)))
            else:
                print(f"   ⚠️ Skipping empty initialization chunk (only contains label comments)")
        
        # Labeled blocks
        for i, (line_num, label_name, _) in enumerate(label_positions):
            end_line = label_positions[i + 1][0] if i + 1 < len(label_positions) else len(lines)
            block_lines = lines[line_num:end_line]
            block_content = '\n'.join(block_lines)
            safe_name = label_name.lower()
            block_key = f"{i+1:02d}_{safe_name}"
            blocks.append((block_key, block_content, len(block_lines)))
        
        return blocks
    
    # ============================================================================
    # STAGE 2: Sub-chunking large blocks
    # ============================================================================
    
    def extract_code_blocks(self, lines: List[str]) -> Tuple[List[str], List[CodeBlock], List[Tuple[int, List[str]]]]:
        """Extract code blocks from large chunk and preserve inter-block content with positions"""
        prologue = []
        code_blocks = []
        inter_block_sections = []  # List of (block_index, lines) tuples
        current_block_lines = []
        current_inter_lines = []
        current_block_type = None
        current_block_name = None
        current_start_line = 0
        in_block = False
        block_counter = 0
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Check for block start markers
            if re.search(self.block_markers['TEMP_TABLE_START'], line_stripped):
                # Save any inter-block content before this block
                if current_inter_lines:
                    inter_block_sections.append((len(code_blocks), current_inter_lines[:]))
                    current_inter_lines = []
                
                if in_block and current_block_lines:
                    self._save_block(code_blocks, current_start_line, i-1, current_block_type, 
                                   current_block_name, current_block_lines)
                block_counter += 1
                current_block_type = 'TEMP_TABLE'
                current_block_name = f"temp_table_{block_counter}"
                current_start_line = i
                current_block_lines = [line]
                in_block = True
            
            elif re.search(self.block_markers['RESULT_TABLE_START'], line_stripped):
                # Save any inter-block content before this block
                if current_inter_lines:
                    inter_block_sections.append((len(code_blocks), current_inter_lines[:]))
                    current_inter_lines = []
                
                if in_block and current_block_lines:
                    self._save_block(code_blocks, current_start_line, i-1, current_block_type,
                                   current_block_name, current_block_lines)
                block_counter += 1
                current_block_type = 'RESULT_TABLE'
                current_block_name = f"result_table_{block_counter}"
                current_start_line = i
                current_block_lines = [line]
                in_block = True
            
            elif re.search(self.block_markers['RESULTVIEW_START'], line_stripped):
                # Save any inter-block content before this block
                if current_inter_lines:
                    inter_block_sections.append((len(code_blocks), current_inter_lines[:]))
                    current_inter_lines = []
                
                if in_block and current_block_lines:
                    self._save_block(code_blocks, current_start_line, i-1, current_block_type,
                                   current_block_name, current_block_lines)
                block_counter += 1
                current_block_type = 'RESULT_VIEW'
                current_block_name = f"result_view_{block_counter}"
                current_start_line = i
                current_block_lines = [line]
                in_block = True
            
            elif re.search(self.block_markers['RESULTABLE_START'], line_stripped):
                # Save any inter-block content before this block
                if current_inter_lines:
                    inter_block_sections.append((len(code_blocks), current_inter_lines[:]))
                    current_inter_lines = []
                
                if in_block and current_block_lines:
                    self._save_block(code_blocks, current_start_line, i-1, current_block_type,
                                   current_block_name, current_block_lines)
                block_counter += 1
                current_block_type = 'RESULTABLE'
                current_block_name = f"resultable_{block_counter}"
                current_start_line = i
                current_block_lines = [line]
                in_block = True
            
            elif re.search(self.block_markers['NESTED_JOB_START'], line_stripped):
                # Save any inter-block content before this block
                if current_inter_lines:
                    inter_block_sections.append((len(code_blocks), current_inter_lines[:]))
                    current_inter_lines = []
                
                if in_block and current_block_lines:
                    self._save_block(code_blocks, current_start_line, i-1, current_block_type,
                                   current_block_name, current_block_lines)
                block_counter += 1
                current_block_type = 'NESTED_JOB'
                current_block_name = f"nested_job_{block_counter}"
                current_start_line = i
                current_block_lines = [line]
                in_block = True
            
            elif re.search(self.block_markers['SECTION_START'], line_stripped):
                # Save any inter-block content before this block
                if current_inter_lines:
                    inter_block_sections.append((len(code_blocks), current_inter_lines[:]))
                    current_inter_lines = []
                
                if in_block and current_block_lines:
                    self._save_block(code_blocks, current_start_line, i-1, current_block_type,
                                   current_block_name, current_block_lines)
                block_counter += 1
                name_match = re.search(r'NAME:(\w+)', line_stripped)
                section_name = name_match.group(1) if name_match else f"section_{block_counter}"
                current_block_type = 'SECTION'
                current_block_name = section_name
                current_start_line = i
                current_block_lines = [line]
                in_block = True
            
            elif in_block:
                current_block_lines.append(line)
                
                if (re.search(self.block_markers['TEMP_TABLE_END'], line_stripped) or
                    re.search(self.block_markers['RESULT_TABLE_END'], line_stripped) or
                    re.search(self.block_markers['SECTION_END'], line_stripped) or
                    re.search(self.block_markers['NESTED_JOB_END'], line_stripped) or
                    re.search(self.block_markers['RESULTVIEW_END'], line_stripped) or
                    re.search(self.block_markers['RESULTABLE_END'], line_stripped)):
                    
                    self._save_block(code_blocks, current_start_line, i, current_block_type,
                                   current_block_name, current_block_lines)
                    in_block = False
                    current_block_lines = []
            
            else:
                # Capture lines not in recognized blocks
                if not code_blocks:
                    prologue.append(line)  # Before first block
                else:
                    current_inter_lines.append(line)  # Between blocks
        
        if in_block and current_block_lines:
            self._save_block(code_blocks, current_start_line, len(lines)-1, current_block_type,
                           current_block_name, current_block_lines)
        
        # Save any remaining inter-block content
        if current_inter_lines:
            inter_block_sections.append((len(code_blocks), current_inter_lines))
        
        return prologue, code_blocks, inter_block_sections
    
    def _save_block(self, code_blocks, start, end, block_type, name, lines):
        if lines:
            code_blocks.append(CodeBlock(start, end, block_type, name, lines))
    
    def split_by_drop_table(self, lines: List[str], block_name: str) -> List[Tuple[str, str, int]]:
        """Fallback: Split block by DROP TABLE boundaries"""
        sub_chunks = []
        drop_positions = []
        
        # Find all DROP TABLE positions
        for i, line in enumerate(lines):
            if re.search(r'^\s*DROP\s+TABLE', line, re.IGNORECASE):
                drop_positions.append(i)
        
        if not drop_positions:
            # No DROP TABLE found, split by line count
            chunk_index = 1
            for i in range(0, len(lines), self.line_threshold):
                chunk_lines = lines[i:i + self.line_threshold]
                chunk_name = f"{block_name}_part{chunk_index:02d}"
                chunk_content = '\n'.join(chunk_lines)
                sub_chunks.append((chunk_name, chunk_content, len(chunk_lines)))
                chunk_index += 1
            return sub_chunks
        
        # Split at DROP TABLE boundaries
        current_chunk_lines = []
        current_start = 0
        chunk_index = 1
        
        for drop_idx in drop_positions:
            # Check if adding this DROP TABLE unit would exceed threshold
            if current_chunk_lines and len(current_chunk_lines) + (drop_idx - current_start) > self.line_threshold:
                # Save current chunk
                chunk_name = f"{block_name}_part{chunk_index:02d}"
                chunk_content = '\n'.join(current_chunk_lines)
                sub_chunks.append((chunk_name, chunk_content, len(current_chunk_lines)))
                chunk_index += 1
                current_chunk_lines = []
            
            # Find end of this DROP TABLE unit (COLLECT STATISTICS + error check)
            unit_end = drop_idx + 1
            for j in range(drop_idx + 1, len(lines)):
                current_chunk_lines.append(lines[j-1] if not current_chunk_lines else lines[j])
                if re.search(r'^\s*COLLECT\s+STATISTICS', lines[j], re.IGNORECASE):
                    # Include COLLECT STATISTICS and next few lines (error check)
                    for k in range(j, min(j + 5, len(lines))):
                        if k > j:
                            current_chunk_lines.append(lines[k])
                        if re.search(r'\.If\s+errorcode', lines[k], re.IGNORECASE):
                            unit_end = k + 1
                            break
                    break
            
            current_start = unit_end
        
        # Add remaining lines
        if current_start < len(lines):
            current_chunk_lines.extend(lines[current_start:])
        
        if current_chunk_lines:
            chunk_name = f"{block_name}_part{chunk_index:02d}"
            chunk_content = '\n'.join(current_chunk_lines)
            sub_chunks.append((chunk_name, chunk_content, len(current_chunk_lines)))
        
        return sub_chunks
    
    def create_sub_chunks(self, prologue: List[str], blocks: List[CodeBlock], 
                         inter_block_sections: List[Tuple[int, List[str]]], block_name: str) -> List[Tuple[str, str, int]]:
        """Create sub-chunks from code blocks, preserving chronological order"""
        sub_chunks = []
        current_chunk_lines = []
        current_chunk_size = 0
        chunk_index = 1
        
        # Convert inter_block_sections to a dict for easy lookup
        inter_dict = {idx: lines for idx, lines in inter_block_sections}
        
        # Add prologue
        if prologue:
            current_chunk_lines.extend(prologue)
            current_chunk_size = len(prologue)
        
        # Process blocks and inter-block content in order
        for block_idx, block in enumerate(blocks):
            # Check if we need to split before adding this block
            if current_chunk_size + block.line_count > self.line_threshold and current_chunk_lines:
                chunk_name = f"{block_name}_part{chunk_index:02d}"
                chunk_content = '\n'.join(current_chunk_lines)
                sub_chunks.append((chunk_name, chunk_content, current_chunk_size))
                chunk_index += 1
                current_chunk_lines = []
                current_chunk_size = 0
            
            # Add the block
            current_chunk_lines.extend(block.lines)
            current_chunk_size += block.line_count
            
            # Add inter-block content that appears AFTER this block
            if (block_idx + 1) in inter_dict:
                inter_lines = inter_dict[block_idx + 1]
                current_chunk_lines.extend(inter_lines)
                current_chunk_size += len(inter_lines)
        
        if current_chunk_lines:
            chunk_name = f"{block_name}_part{chunk_index:02d}"
            chunk_content = '\n'.join(current_chunk_lines)
            sub_chunks.append((chunk_name, chunk_content, current_chunk_size))
        
        return sub_chunks
    
    def has_unconditional_goto_at_end(self, chunk_lines: List[str]) -> bool:
        """Check if chunk ends with unconditional .goto (including --CONTINUE: comments)"""
        # Look at last few non-empty lines
        for i in range(len(chunk_lines) - 1, max(-1, len(chunk_lines) - 10), -1):
            if i >= 0 and chunk_lines[i].strip():
                line = chunk_lines[i].strip()
                
                # Check for --CONTINUE: comment (converted unconditional .goto)
                if re.search(r'^\s*--CONTINUE:\s*\w+', line, re.IGNORECASE):
                    return True
                    
                # Check for actual unconditional .goto/.GOTO
                if re.search(r'^\s*\.goto\s+\w+', line, re.IGNORECASE):
                    return True
                    
                # Stop searching if we hit significant non-comment line
                if not line.startswith('/*') and not line.startswith('--') and not line.startswith('.If'):
                    break
        return False
    
    def process_and_write_chunks(self, stage1_blocks: List[Tuple[str, str, int]]):
        """Process blocks and write final chunks to single output folder with cross-chunk .goto logic"""
        print(f"\n{'='*60}")
        print(f"PROCESSING AND WRITING CHUNKS")
        print(f"{'='*60}")
        
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        large_count = 0
        small_count = 0
        total_chunks = 0
        final_chunks = []  # Store all chunks for cross-chunk .goto analysis
        
        # First pass: Create all chunks
        for block_name, content, line_count in stage1_blocks:
            if line_count > self.line_threshold:
                large_count += 1
                print(f"\n🔧 Sub-chunking: {block_name} ({line_count:,} lines)")
                
                lines = content.split('\n')
                prologue, code_blocks, inter_block_sections = self.extract_code_blocks(lines)
                
                if code_blocks:
                    print(f"   Strategy: Structured block markers ({len(code_blocks)} blocks found)")
                    if inter_block_sections:
                        total_inter_lines = sum(len(lines) for _, lines in inter_block_sections)
                        print(f"   Additional unrecognized content: {total_inter_lines} lines in {len(inter_block_sections)} sections")
                    sub_chunks = self.create_sub_chunks(prologue, code_blocks, inter_block_sections, block_name)
                else:
                    print(f"   Strategy: DROP TABLE boundaries (fallback)")
                    sub_chunks = self.split_by_drop_table(lines, block_name)
                
                if len(sub_chunks) > 1:
                    for sub_name, sub_content, sub_lines in sub_chunks:
                        final_chunks.append((sub_name, sub_content, sub_lines))
                        print(f"   → {sub_name}.sql ({sub_lines:,} lines)")
                else:
                    # Single chunk, keep original name
                    final_chunks.append((block_name, content, line_count))
                    print(f"   → {block_name}.sql (single chunk, no split needed)")
            else:
                # Small block, copy as-is
                small_count += 1
                final_chunks.append((block_name, content, line_count))
        
        # Second pass: Add smart .goto between chunks and write files
        print(f"\n🔗 Cross-chunk .goto analysis:")
        gotos_added = 0
        
        for i, (chunk_name, chunk_content, line_count) in enumerate(final_chunks):
            # Check if next chunk needs .goto
            if i + 1 < len(final_chunks):
                next_chunk_name, next_chunk_content, _ = final_chunks[i + 1]
                next_chunk_lines = next_chunk_content.split('\n')
                
                # Find first non-empty line in next chunk
                next_first_line = ""
                for line in next_chunk_lines:
                    if line.strip():
                        next_first_line = line.strip()
                        break
                
                # Check if next chunk starts with .label
                label_match = re.search(r'^\s*\.label\s+(\w+)', next_first_line, re.IGNORECASE)
                if label_match:
                    next_label_name = label_match.group(1).upper()
                    
                    # Check if current chunk ends with unconditional .goto (including --CONTINUE:)
                    current_chunk_lines = chunk_content.split('\n')
                    has_unconditional_goto_at_end = self.has_unconditional_goto_at_end(current_chunk_lines)
                    
                    # Add .goto if no unconditional .goto found at end
                    if not has_unconditional_goto_at_end:
                        print(f"   🔗 Adding .goto {next_label_name} to end of {chunk_name}")
                        gotos_added += 1
                        
                        # Find last .If errorcode for proper indentation
                        errorcode_indent = "\t"
                        for j in range(len(current_chunk_lines) - 1, -1, -1):
                            if re.search(r'^\s*\.If\s+errorcode', current_chunk_lines[j], re.IGNORECASE):
                                errorcode_indent = re.match(r'^(\s*)', current_chunk_lines[j]).group(1)
                                break
                        
                        # Add .goto at end
                        current_chunk_lines.append("")
                        current_chunk_lines.append(f"{errorcode_indent}.goto {next_label_name}")
                        chunk_content = '\n'.join(current_chunk_lines)
                    else:
                        print(f"   ✓ {chunk_name} already ends with unconditional .goto or --CONTINUE: - no addition needed")
            
            # Write the chunk
            output_file = self.output_folder / f"{chunk_name}.sql"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(chunk_content)
            
            total_chunks += 1
        
        print(f"   Cross-chunk .gotos added: {gotos_added}")
        print(f"\n✓ Sub-chunked {large_count} large blocks")
        print(f"✓ Copied {small_count} small blocks unchanged")
        print(f"✓ Total chunks created: {total_chunks}")
    
    def run(self):
        """Execute complete pipeline"""
        print(f"\n{'='*60}")
        print(f"BTEQ COMPLETE CHUNKING PIPELINE")
        print(f"{'='*60}")
        print(f"Input: {self.input_file.name}")
        print(f"Threshold: {self.line_threshold} lines")
        
        # Stage 1: Label-based chunking (in memory)
        print(f"\n{'='*60}")
        print(f"STAGE 1: Label-based chunking")
        print(f"{'='*60}")
        
        self.read_file()
        self.process_labels()
        stage1_blocks = self.extract_label_blocks()
        
        print(f"\n✓ Stage 1 complete: {len(stage1_blocks)} blocks identified")
        
        # Stage 2: Sub-chunking and writing to single folder
        self.process_and_write_chunks(stage1_blocks)
        
        print(f"\n{'='*60}")
        print(f"✅ PIPELINE COMPLETE!")
        print(f"{'='*60}")
        print(f"📁 Output folder: {self.output_folder}")
        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description='Complete BTEQ chunking pipeline (label + sub-chunking)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('input_file', nargs='?',
                       default=r"{file_path}",
                       help='Path to input BTEQ script')
    parser.add_argument('--threshold', type=int, default=1200,
                       help='Maximum lines per chunk (default: 1200)')
    
    args = parser.parse_args()
    
    try:
        pipeline = BteqCompletePipeline(args.input_file, args.threshold)
        pipeline.run()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
