/**
 * boilerpipe
 * <p>
 * Copyright (c) 2009, 2014 Christian Kohlschütter
 * <p>
 * The author licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kohlschutter.boilerpipe.filters.heuristics;

import com.kohlschutter.boilerpipe.BoilerpipeFilter;
import com.kohlschutter.boilerpipe.BoilerpipeProcessingException;
import com.kohlschutter.boilerpipe.document.TextBlock;
import com.kohlschutter.boilerpipe.document.TextDocument;
import com.kohlschutter.boilerpipe.labels.DefaultLabels;

import java.util.List;
import java.util.ListIterator;

/**
 * Keeps the largest {@link TextBlock} only (by the number of words). In case of more than one block
 * with the same number of words, the first block is chosen. All discarded blocks are marked
 * "not content" and flagged as {@link DefaultLabels#MIGHT_BE_CONTENT}.
 *
 * Note that, by default, only TextBlocks marked as "content" are taken into consideration.
 */
public final class KeepLargestBlockFilter implements BoilerpipeFilter {
  public static final KeepLargestBlockFilter INSTANCE = new KeepLargestBlockFilter(false, 0);
  public static final KeepLargestBlockFilter INSTANCE_EXPAND_TO_SAME_TAGLEVEL =
      new KeepLargestBlockFilter(true, 0);
  public static final KeepLargestBlockFilter INSTANCE_EXPAND_TO_SAME_TAGLEVEL_MIN_WORDS =
      new KeepLargestBlockFilter(true, 150);
  private final boolean expandToSameLevelText;
  private final int minWords;

  public KeepLargestBlockFilter(boolean expandToSameLevelText, final int minWords) {
    this.expandToSameLevelText = expandToSameLevelText;
    this.minWords = minWords;
  }

  public boolean process(final TextDocument doc) throws BoilerpipeProcessingException {
    List<TextBlock> textBlocks = doc.getTextBlocks();
    if (textBlocks.size() < 2) {
      return false;
    }

    TextBlock largestBlock = findLargestBlock(textBlocks);
    if (largestBlock == null) {
      return false; // No content blocks found
    }

    int n = textBlocks.indexOf(largestBlock);
    markBlocks(textBlocks, largestBlock);

    if (expandToSameLevelText && n != -1) {
      expandToSameTagLevel(textBlocks, n, largestBlock.getTagLevel());
    }

    return true;
  }

  private TextBlock findLargestBlock(List<TextBlock> textBlocks) {
    int maxNumWords = -1;
    TextBlock largestBlock = null;
    for (TextBlock tb : textBlocks) {
      if (tb.isContent()) {
        final int nw = tb.getNumWords();
        if (nw > maxNumWords) {
          largestBlock = tb;
          maxNumWords = nw;
        }
      }
    }
    return largestBlock;
  }

  private void markBlocks(List<TextBlock> textBlocks, TextBlock largestBlock) {
    for (TextBlock tb : textBlocks) {
      if (tb == largestBlock) {
        tb.setIsContent(true);
        tb.addLabel(DefaultLabels.VERY_LIKELY_CONTENT);
      } else {
        tb.setIsContent(false);
        tb.addLabel(DefaultLabels.MIGHT_BE_CONTENT);
      }
    }
  }

  private void expandToSameTagLevel(List<TextBlock> textBlocks, int startIndex, int targetLevel) {
    expandPreviousBlocks(textBlocks, startIndex, targetLevel);
    expandNextBlocks(textBlocks, startIndex, targetLevel);
  }

  private void expandPreviousBlocks(List<TextBlock> textBlocks, int startIndex, int targetLevel) {
    for (ListIterator<TextBlock> it = textBlocks.listIterator(startIndex); it.hasPrevious(); ) {
      TextBlock tb = it.previous();
      final int tl = tb.getTagLevel();
      if (tl < targetLevel) {
        break;
      } else if (tl == targetLevel && tb.getNumWords() >= minWords) {
        tb.setIsContent(true);
      }
    }
  }

  private void expandNextBlocks(List<TextBlock> textBlocks, int startIndex, int targetLevel) {
    for (ListIterator<TextBlock> it = textBlocks.listIterator(startIndex); it.hasNext(); ) {
      TextBlock tb = it.next();
      final int tl = tb.getTagLevel();
      if (tl < targetLevel) {
        break;
      } else if (tl == targetLevel && tb.getNumWords() >= minWords) {
        tb.setIsContent(true);
      }
    }
  }
//Refactoring end